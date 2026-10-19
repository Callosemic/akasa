import os
import stat
import time
import json
import shutil
import queue
import traceback
import threading
from collections import OrderedDict
from pathlib import Path
from akasa.hooks import hooks
from akasa.db import get_connection
from akasa.uri import AkasaURI
from akasa.utils import resolve_system_artifact_path, parse_uri, get_tenant_control_dir, get_all_workspace_ids, is_subpath
_VFS_READ_CACHE = OrderedDict()
_VFS_READ_CACHE_BYTES = 0
_VFS_READ_CACHE_MAX_BYTES = 8 * 1024 * 1024  # 8 MB RAM cache
_VFS_READ_CACHE_MAX_FILE_BYTES = 1 * 1024 * 1024  # Only cache individual files <= 1 MB
_VFS_CACHE_LOCK = threading.Lock()
def _cache_get(resolved_path: str, st=None):
    """Returns cached file text if physical (mtime_ns, size) matches; otherwise None."""
    global _VFS_READ_CACHE_BYTES
    if st is None:
        try:
            st = os.stat(resolved_path)
        except OSError:
            with _VFS_CACHE_LOCK:
                entry = _VFS_READ_CACHE.pop(resolved_path, None)
                if entry:
                    _VFS_READ_CACHE_BYTES -= entry[2]
            return None

    with _VFS_CACHE_LOCK:
        entry = _VFS_READ_CACHE.get(resolved_path)
        if entry and entry[0] == st.st_mtime_ns and entry[1] == st.st_size:
            _VFS_READ_CACHE.move_to_end(resolved_path)
            return entry[3]
    return None

def _cache_put(resolved_path: str, content: str, st=None):
    """Updates the write-through LRU cache with the current physical stat signature."""
    global _VFS_READ_CACHE_BYTES
    if st is None:
        try:
            st = os.stat(resolved_path)
        except OSError:
            return

    content_bytes = st.st_size
    if content_bytes > _VFS_READ_CACHE_MAX_FILE_BYTES:
        _cache_evict(resolved_path)
        return
    with _VFS_CACHE_LOCK:
        old = _VFS_READ_CACHE.pop(resolved_path, None)
        if old:
            _VFS_READ_CACHE_BYTES -= old[2]
        while _VFS_READ_CACHE and (_VFS_READ_CACHE_BYTES + content_bytes > _VFS_READ_CACHE_MAX_BYTES):
            _, evicted = _VFS_READ_CACHE.popitem(last=False)
            _VFS_READ_CACHE_BYTES -= evicted[2]
        _VFS_READ_CACHE[resolved_path] = (st.st_mtime_ns, st.st_size, content_bytes, content)
        _VFS_READ_CACHE_BYTES += content_bytes
def _cache_evict(resolved_path: str):
    global _VFS_READ_CACHE_BYTES
    if not resolved_path:
        return
    with _VFS_CACHE_LOCK:
        old = _VFS_READ_CACHE.pop(resolved_path, None)
        if old:
            _VFS_READ_CACHE_BYTES -= old[2]
        dir_prefix = resolved_path.rstrip('/') + '/'
        child_keys = [k for k in _VFS_READ_CACHE.keys() if k.startswith(dir_prefix)]
        for k in child_keys:
            entry = _VFS_READ_CACHE.pop(k, None)
            if entry:
                _VFS_READ_CACHE_BYTES -= entry[2]

def _is_file_identical(resolved_path: str, content: str) -> bool:
    """Fast-path idempotency check: single stat + RAM cache check before touching disk."""
    if not resolved_path:
        return False
    try:
        st = os.stat(resolved_path)
    except OSError:
        return False
    if stat.S_ISDIR(st.st_mode):
        return False

    with _VFS_CACHE_LOCK:
        entry = _VFS_READ_CACHE.get(resolved_path)
        if entry and entry[0] == st.st_mtime_ns and entry[1] == st.st_size:
            _VFS_READ_CACHE.move_to_end(resolved_path)
            return entry[3] == content

    char_len = len(content)
    if st.st_size < char_len or st.st_size > char_len * 4:
        return False

    content_bytes = len(content.encode('utf-8'))
    if st.st_size != content_bytes:
        return False

    try:
        with open(resolved_path, 'r', encoding='utf-8') as f:
            disk_text = f.read()
        _cache_put(resolved_path, disk_text, st=st)
        return disk_text == content
    except Exception:
        return False
class DefaultVFSDriver:
    def resolve(self, uri_obj, workspace_id, physical_target):
        target = Path(physical_target).resolve()
        if uri_obj.path:
            resolved = target.joinpath(uri_obj.path).resolve()
            if not is_subpath(resolved, target):
                return target.as_posix()
            return resolved.as_posix()
        return target.as_posix()
_MOUNT_AUTHORIZER = None

def set_mount_authorizer(authorizer_fn):
    """DI Interface: Host OS registers its peer-to-peer mount authorization policy."""
    global _MOUNT_AUTHORIZER
    _MOUNT_AUTHORIZER = authorizer_fn


class InterWorkspaceVFSDriver:
    """
    Zero-copy Inter-Workspace VFS Driver (ws://<source_workspace_id>/<domain>/<relative_path>).
    Delegates authorization to the host-injected _MOUNT_AUTHORIZER policy.
    """
    def resolve(self, uri_obj, workspace_id, physical_target=None):
        target_ws = uri_obj.volume or "default"
        if target_ws != "default" and target_ws not in get_all_workspace_ids():
            return None

        rel_path = (uri_obj.path or "").lstrip('/')
        domain = rel_path.split('/')[0] if '/' in rel_path else rel_path

        if target_ws != workspace_id and _MOUNT_AUTHORIZER is not None:
            if not _MOUNT_AUTHORIZER(target_ws, workspace_id, domain, rel_path):
                return None

        try:
            control_dir = Path(get_tenant_control_dir(target_ws)).resolve()
            data_root = control_dir.joinpath("data").resolve()
        except Exception:
            return None
        if not rel_path:
            return data_root.as_posix()

        resolved = data_root.joinpath(rel_path).resolve()
        return resolved.as_posix() if is_subpath(resolved, data_root) else None


_DRIVERS = {
    'vfs': DefaultVFSDriver(),
    'ws': InterWorkspaceVFSDriver()
}
_MOUNT_TABLE = {}

def register_driver(scheme, driver_instance, volume=None):
    if volume:
        _DRIVERS[(scheme, volume)] = driver_instance
    else:
        _DRIVERS[scheme] = driver_instance

def mount_volume(workspace_id, scheme, volume, physical_target):
    if not volume:
        volume = ""
    _MOUNT_TABLE[(workspace_id, scheme, volume)] = physical_target

def _resolve_physical_path(filepath, workspace_id, is_absolute_artifact=False):
    """Centralized SSOT for VFS path resolution via the Volume Router."""
    if not filepath: return None

    if is_absolute_artifact and Path(filepath).is_absolute():
        return resolve_system_artifact_path(filepath, workspace_id)

    vol, rel = parse_uri(filepath)
    if vol and "://" not in str(filepath):
        uri = AkasaURI(f"vfs://{vol}/{rel}")
    else:
        uri = AkasaURI(filepath)
    scheme = uri.scheme or 'vfs'
    volume = uri.volume or ""

    if scheme == 'ws':
        ws_driver = _DRIVERS.get('ws')
        return ws_driver.resolve(uri, workspace_id, None) if ws_driver else None

    mount_key = (workspace_id, scheme, volume)
    physical_target = _MOUNT_TABLE.get(mount_key)
    if not physical_target:
        hooks.emit('vfs_mount_missing', workspace_id=workspace_id, scheme=scheme, volume=volume)
        physical_target = _MOUNT_TABLE.get(mount_key)
    if not physical_target:
        return None

    driver = _DRIVERS.get((scheme, volume)) or _DRIVERS.get(scheme)
    if not driver:
        return None

    return driver.resolve(uri, workspace_id, physical_target)

_VFS_WRITE_QUEUE = queue.Queue()
_VFS_WORKER_THREAD = None
_VFS_SHUTDOWN_SIGNAL = threading.Event()
def _ensure_vfs_worker_alive():
    """Guarantees the background VFS commit thread is running before queuing tasks or barriers."""
    if _VFS_WORKER_THREAD is None or not _VFS_WORKER_THREAD.is_alive():
        if not _VFS_SHUTDOWN_SIGNAL.is_set():
            start_vfs_pipeline()

def sync_vfs_barrier(workspace_id="default", timeout=5.0):
    """Halts the calling thread until all VFS writes queued prior to this call are physically flushed."""
    _ensure_vfs_worker_alive()
    barrier_event = threading.Event()
    _VFS_WRITE_QUEUE.put((workspace_id, "", "", {"action": "barrier", "barrier_event": barrier_event}))

    settled = barrier_event.wait(timeout=timeout)

    if _VFS_SHUTDOWN_SIGNAL.is_set():
        raise RuntimeError("Transaction aborted mid-flight due to system shutdown or workspace context swap.")
    if not settled:
        raise TimeoutError(f"VFS Barrier Timeout: The async write queue failed to reach the sync watermark within {timeout} seconds.")

def _vfs_commit_worker():
    """Consumes write payloads sequentially off-thread to guard the Flask event loop."""
    while True:
        if _VFS_SHUTDOWN_SIGNAL.is_set() and _VFS_WRITE_QUEUE.empty():
            break
        try:
            task = _VFS_WRITE_QUEUE.get(timeout=1.0)
            if task is None:
                _VFS_WRITE_QUEUE.task_done()
                continue
            workspace_id, filepath, content, data = task
            try:
                action = data.get("action", "save")
                if action == "barrier":
                    if "barrier_event" in data:
                        data["barrier_event"].set()
                    continue
                elif action == "delete":
                    resolved_path = _resolve_physical_path(filepath, workspace_id, data.get("is_absolute_artifact"))

                    try: hooks.emit('pre_file_save', workspace_id=workspace_id, filepath=filepath, resolved_path=resolved_path, content="", data=data)
                    except Exception: pass
                    if resolved_path and os.path.exists(resolved_path):
                        _cache_evict(resolved_path)
                        if os.path.isdir(resolved_path):
                            shutil.rmtree(resolved_path)
                        else:
                            os.remove(resolved_path)
                    ignore_ledger = bool(data.get("is_absolute_artifact") or data.get("ignore_ledger"))
                    if not ignore_ledger:
                        db_conn = get_connection("workers", workspace_id=workspace_id)
                        db_conn.execute(
                            "INSERT OR REPLACE INTO vfs_event_log (filepath, mutation_type, timestamp) VALUES (?, ?, ?)",
                            (filepath, 'deleted', time.time())
                        )
                        db_conn.commit()
                    hooks.emit('vfs_mutated', workspace_id=workspace_id, mutations=[{"filepath": filepath, "operation": "delete", "ignore_ledger": ignore_ledger}])
                elif action == "move":
                    dest_path = data.get("dest_path")
                    resolved_src = _resolve_physical_path(filepath, workspace_id, data.get("is_absolute_artifact"))
                    resolved_dest = _resolve_physical_path(dest_path, workspace_id, data.get("is_absolute_artifact"))

                    try: 
                        hooks.emit('pre_file_save', workspace_id=workspace_id, filepath=filepath, resolved_path=resolved_src, content="", data=data)
                        hooks.emit('pre_file_save', workspace_id=workspace_id, filepath=dest_path, resolved_path=resolved_dest, content="", data=data)
                    except Exception: pass

                    if resolved_src and resolved_dest and os.path.exists(resolved_src):
                        _cache_evict(resolved_src)
                        _cache_evict(resolved_dest)
                        os.makedirs(Path(resolved_dest).parent, exist_ok=True)
                        shutil.move(resolved_src, resolved_dest)

                        ignore_ledger = bool(data.get("is_absolute_artifact") or data.get("ignore_ledger"))
                        if not ignore_ledger:
                            db_conn = get_connection("workers", workspace_id=workspace_id)
                            db_conn.execute(
                                "INSERT OR REPLACE INTO vfs_event_log (filepath, mutation_type, timestamp) VALUES (?, ?, ?)",
                                (filepath, 'deleted', time.time())
                            )
                            db_conn.execute(
                                "INSERT OR REPLACE INTO vfs_event_log (filepath, mutation_type, timestamp) VALUES (?, ?, ?)",
                                (dest_path, 'added', time.time())
                            )
                            db_conn.commit()
                        hooks.emit('vfs_mutated', workspace_id=workspace_id, mutations=[
                            {"filepath": filepath, "operation": "delete", "ignore_ledger": ignore_ledger},
                            {"filepath": dest_path, "operation": "save", "ignore_ledger": ignore_ledger}
                        ])
                    else:
                        print(f"❌ [VFS Pipeline] Move aborted: Source file missing on disk ({filepath})")
                elif action == "bulk":
                    operations = data.get("operations", [])
                    db_conn = get_connection("workers", workspace_id=workspace_id)
                    now_ts = time.time()
                    ledger_rows = []
                    emitted_mutations = []

                    for op_item in operations:
                        op_type = op_item.get("action")
                        src_fp = op_item.get("filepath", "")
                        dst_fp = op_item.get("dest_path", "")
                        ignore_ledger = bool(op_item.get("is_absolute_artifact") or op_item.get("ignore_ledger"))

                        if op_type == "delete" and src_fp:
                            resolved_path = _resolve_physical_path(src_fp, workspace_id, op_item.get("is_absolute_artifact"))
                            try:
                                hooks.emit('pre_file_save', workspace_id=workspace_id, filepath=src_fp, resolved_path=resolved_path, content="", data=op_item)
                            except Exception:
                                pass
                            if resolved_path and os.path.exists(resolved_path):
                                _cache_evict(resolved_path)
                                if os.path.isdir(resolved_path):
                                    shutil.rmtree(resolved_path)
                                else:
                                    os.remove(resolved_path)
                            if not ignore_ledger:
                                ledger_rows.append((src_fp, 'deleted', now_ts))
                            emitted_mutations.append({"filepath": src_fp, "operation": "delete", "ignore_ledger": ignore_ledger})
                        elif op_type == "move" and src_fp and dst_fp:
                            resolved_src = _resolve_physical_path(src_fp, workspace_id, op_item.get("is_absolute_artifact"))
                            resolved_dest = _resolve_physical_path(dst_fp, workspace_id, op_item.get("is_absolute_artifact"))
                            try:
                                hooks.emit('pre_file_save', workspace_id=workspace_id, filepath=src_fp, resolved_path=resolved_src, content="", data=op_item)
                                hooks.emit('pre_file_save', workspace_id=workspace_id, filepath=dst_fp, resolved_path=resolved_dest, content="", data=op_item)
                            except Exception:
                                pass
                            if resolved_src and resolved_dest and os.path.exists(resolved_src):
                                _cache_evict(resolved_src)
                                _cache_evict(resolved_dest)
                                os.makedirs(Path(resolved_dest).parent, exist_ok=True)
                                shutil.move(resolved_src, resolved_dest)
                                if not ignore_ledger:
                                    ledger_rows.append((src_fp, 'deleted', now_ts))
                                    ledger_rows.append((dst_fp, 'added', now_ts))
                                emitted_mutations.append({"filepath": src_fp, "operation": "delete", "ignore_ledger": ignore_ledger})
                                emitted_mutations.append({"filepath": dst_fp, "operation": "save", "ignore_ledger": ignore_ledger})

                        elif op_type == "save" and src_fp:
                            save_res = execute_vfs_save_physical(workspace_id, src_fp, op_item.get("content", ""), op_item, defer_events=True)
                            if save_res:
                                s_rows, s_muts = save_res
                                ledger_rows.extend(s_rows)
                                emitted_mutations.extend(s_muts)

                    if ledger_rows:
                        db_conn.executemany(
                            "INSERT OR REPLACE INTO vfs_event_log (filepath, mutation_type, timestamp) VALUES (?, ?, ?)",
                            ledger_rows
                        )
                        db_conn.commit()
                    if emitted_mutations:
                        hooks.emit('vfs_mutated', workspace_id=workspace_id, mutations=emitted_mutations)
                else:
                    execute_vfs_save_physical(workspace_id, filepath, content, data)
            except Exception as e:
                print(f"❌ [VFS Pipeline] Background operation failed for {filepath}: {e}")
                try:
                    hooks.emit_background('system_error', source="VFS Pipeline", error_type=type(e).__name__, message=str(e), traceback=traceback.format_exc(), payload=json.dumps(data) if isinstance(data, dict) else json.dumps({"data": str(data)}), workspace_id=workspace_id)
                except Exception: pass
            finally:
                _VFS_WRITE_QUEUE.task_done()
        except queue.Empty:
            continue

@hooks.on('system_boot')
def start_vfs_pipeline(**kwargs):
    global _VFS_WORKER_THREAD
    if _VFS_WORKER_THREAD is not None and _VFS_WORKER_THREAD.is_alive():
        return
    _VFS_SHUTDOWN_SIGNAL.clear()
    _VFS_WORKER_THREAD = threading.Thread(target=_vfs_commit_worker, name="VFS-Commit-Pipeline", daemon=True)
    _VFS_WORKER_THREAD.start()
    print("🚀 Asynchronous VFS Commit Pipeline Online.")

@hooks.on('system_shutdown')
def stop_vfs_pipeline(**kwargs):
    print("🛑 Draining VFS Commit Pipeline...")
    _VFS_SHUTDOWN_SIGNAL.set()
    _VFS_WRITE_QUEUE.put(None)
    if _VFS_WORKER_THREAD:
        _VFS_WORKER_THREAD.join(timeout=5.0)
def execute_vfs_move(workspace_id, filepath, dest_path):
    if str(filepath).startswith("ws://") or str(dest_path).startswith("ws://"):
        return {"status": "error", "message": "Cross-workspace virtual mounts ('ws://') are strictly read-only."}, 403
    _ensure_vfs_worker_alive()
    _VFS_WRITE_QUEUE.put((workspace_id, filepath, "", {"action": "move", "dest_path": dest_path}))
    return {"status": "accepted", "message": f"File move queued."}, 202
def execute_vfs_delete(workspace_id, filepath):
    if str(filepath).startswith("ws://"):
        return {"status": "error", "message": "Cross-workspace virtual mounts ('ws://') are strictly read-only."}, 403
    _ensure_vfs_worker_alive()
    _VFS_WRITE_QUEUE.put((workspace_id, filepath, "", {"action": "delete"}))
    return {"status": "accepted", "message": f"File deletion queued."}, 202

def execute_vfs_bulk(workspace_id, operations):
    sanitized = []
    for op in operations:
        fp = op.get("filepath", "")
        dest = op.get("dest_path", "")
        if str(fp).startswith("ws://") or str(dest).startswith("ws://"):
            continue
        sanitized.append(op)
    _ensure_vfs_worker_alive()
    _VFS_WRITE_QUEUE.put((workspace_id, "", "", {"action": "bulk", "operations": sanitized}))
    return {"status": "accepted", "message": f"Bulk operation ({len(sanitized)} item(s)) queued.", "operations": sanitized}, 202
def execute_vfs_save(workspace_id, filepath, content, data=None):
    if data is None:
        data = {}
    if str(filepath).startswith("ws://"):
        print(f"🛑 [VFS Security] Write dropped: Virtual mount '{filepath}' is strictly read-only.")
        return {"status": "error", "message": "Cross-workspace virtual mounts ('ws://') are strictly read-only."}
    # Pre-flight Volume Validation
    if not data.get("is_absolute_artifact"):
        if not _resolve_physical_path(filepath, workspace_id):
            print(f"⚠️ [VFS] Save dropped: Volume for '{filepath}' is not mounted.")
            return {"status": "error", "message": f"Volume for '{filepath}' is not mounted."}

    _ensure_vfs_worker_alive()
    _VFS_WRITE_QUEUE.put((workspace_id, filepath, content, data))
    return {"status": "accepted", "message": f"File {filepath} queued for atomic background commit."}
def execute_vfs_save_physical(workspace_id, filepath, content, data, defer_events=False):
    resolved_path = _resolve_physical_path(filepath, workspace_id, data.get("is_absolute_artifact"))

    try:
        hooks.emit('pre_file_save', workspace_id=workspace_id, filepath=filepath, resolved_path=resolved_path, content=content, data=data)
    except Exception:
        pass

    if not resolved_path:
        print(f"❌ [VFS Pipeline] Save aborted: Unable to resolve physical path for '{filepath}' on workspace '{workspace_id}'")
        return None

    is_new = not os.path.exists(resolved_path)
    # Idempotency Gatekeeper: Fast-path size + RAM cache check before touching disk
    if not is_new and not data.get("delete_source"):
        if _is_file_identical(resolved_path, content):
            if os.environ.get("AKASA_DEBUG") == "1":
                print(f"💽 [VFS TELEMETRY] Silent drop (idempotent): {filepath}")
            return None

    if os.environ.get("AKASA_DEBUG") == "1":
        print(f"💽 [VFS TELEMETRY] Writing to disk: {filepath}")

    target_dir = Path(resolved_path).parent.as_posix()
    if target_dir:
        os.makedirs(target_dir, exist_ok=True)
    with open(resolved_path, 'w', encoding='utf-8') as f:
        f.write(content)
    _cache_put(resolved_path, content)

    now_ts = time.time()
    ignore_ledger = bool(data.get("is_absolute_artifact") or data.get("ignore_ledger"))
    ledger_rows = []
    emitted_mutations = []

    if not ignore_ledger:
        ledger_rows.append((filepath, 'modified' if not is_new else 'added', now_ts))

    delete_source = data.get("delete_source")
    if delete_source:
        old_abs_path = _resolve_physical_path(delete_source, workspace_id, data.get("is_absolute_artifact"))
        if old_abs_path and os.path.exists(old_abs_path) and os.path.abspath(old_abs_path) != os.path.abspath(resolved_path):
            _cache_evict(old_abs_path)
            os.remove(old_abs_path)
            emitted_mutations.append({"filepath": delete_source, "operation": "delete", "ignore_ledger": ignore_ledger})
            try:
                parent_dir = Path(old_abs_path).parent.as_posix()
                while parent_dir and os.path.isdir(parent_dir) and not os.listdir(parent_dir):
                    os.rmdir(parent_dir)
                    parent_dir = Path(parent_dir).parent.as_posix()
            except OSError:
                pass
            if not ignore_ledger:
                ledger_rows.append((delete_source, 'deleted', now_ts))

    emitted_mutations.append({"filepath": filepath, "operation": "save", "ignore_ledger": ignore_ledger})

    if defer_events:
        return ledger_rows, emitted_mutations

    if ledger_rows:
        db_conn = get_connection("workers", workspace_id=workspace_id)
        db_conn.executemany(
            "INSERT OR REPLACE INTO vfs_event_log (filepath, mutation_type, timestamp) VALUES (?, ?, ?)",
            ledger_rows
        )
        db_conn.commit()
    if os.environ.get("AKASA_DEBUG") == "1":
        print(f"💽 [VFS TELEMETRY] Emitting vfs_mutated for {filepath} (ignore_ledger={ignore_ledger})")
    hooks.emit('vfs_mutated', workspace_id=workspace_id, mutations=emitted_mutations)
    return None
class VFSTransaction:
    """Provides atomic-style batching and async queue dispatch for file mutations."""
    def __init__(self, workspace_id):
        self.workspace_id = workspace_id
        self._buffer = []
        self._in_transaction = False
    def save(self, filepath, content, data=None):
        data = data or {}
        if not data.get("is_absolute_artifact") and filepath and "://" not in str(filepath):
            raise ValueError(f"Non-canonical URI rejected by VFS save (missing scheme:// prefix): '{filepath}'")
        resolved = _resolve_physical_path(filepath, self.workspace_id, data.get("is_absolute_artifact"))
        if not data.get("is_absolute_artifact") and not resolved:
            print(f"⚠️ [VFS] Transaction dropped: Volume for '{filepath}' is not mounted.")
            return
        if self._in_transaction:
            self._buffer.append((filepath, content, data))
            return
        # Pre-flight Idempotency Gatekeeper: fast-path size + RAM cache check
        if not data.get("delete_source") and resolved and _is_file_identical(resolved, content):
            return
        execute_vfs_save(self.workspace_id, filepath, content, data)

    def delete(self, filepath, data=None):
        """Ergonomic wrapper for queuing an atomic VFS deletion."""
        data = data or {}
        if not data.get("is_absolute_artifact") and filepath and "://" not in str(filepath):
            raise ValueError(f"Non-canonical URI rejected by VFS delete (missing scheme:// prefix): '{filepath}'")
        data["action"] = "delete"
        if self._in_transaction:
            self._buffer.append((filepath, "", data))
        else:
            execute_vfs_save(self.workspace_id, filepath, "", data)
    def read(self, filepath, is_absolute_artifact=False):
        """Safely resolves and reads a file's contents with a single stat syscall."""
        if not is_absolute_artifact and filepath and "://" not in str(filepath):
            raise ValueError(f"Non-canonical URI rejected by VFS read (missing scheme:// prefix): '{filepath}'")
        if self._in_transaction and self._buffer:
            for buf_fp, buf_content, buf_data in reversed(self._buffer):
                if buf_fp == filepath:
                    if buf_data.get("action") == "delete":
                        return None
                    return buf_content

        resolved = _resolve_physical_path(filepath, self.workspace_id, is_absolute_artifact)
        if not resolved:
            return None

        try:
            st = os.stat(resolved)
        except OSError:
            return None

        if stat.S_ISDIR(st.st_mode):
            return None

        cached = _cache_get(resolved, st=st)
        if cached is not None:
            return cached
        try:
            with open(resolved, 'r', encoding='utf-8') as f:
                text = f.read()
            _cache_put(resolved, text, st=st)
            return text
        except (UnicodeDecodeError, OSError):
            return None

    def __enter__(self):
        self._in_transaction = True
        self._buffer = []
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        self._in_transaction = False
        if exc_type is None and self._buffer:
            if len(self._buffer) == 1:
                filepath, content, data = self._buffer[0]
                execute_vfs_save(self.workspace_id, filepath, content, data)
            else:
                bulk_ops = []
                for filepath, content, data in self._buffer:
                    op_data = dict(data or {})
                    op_data["action"] = op_data.get("action", "save")
                    op_data["filepath"] = filepath
                    op_data["content"] = content
                    bulk_ops.append(op_data)
                execute_vfs_bulk(self.workspace_id, bulk_ops)

        self._buffer = []
    def walk(self, directory_path, exts=None):
        """Safely sweeps a directory within the workspace bounds, yielding strict workspace-relative file paths."""
        resolved_dir = _resolve_physical_path(directory_path, self.workspace_id)

        if not resolved_dir or not os.path.exists(resolved_dir):
            return
        for root, dirs, files in os.walk(resolved_dir):
            # Inversion of Control: Host application defines standard noise to ignore
            ignored_dirs = next((res for res in hooks.emit('request_vfs_ignore_dirs', workspace_id=self.workspace_id) if res), [])
            dirs[:] = [d for d in dirs if d not in ignored_dirs]
            for file in files:
                if exts and not any(file.endswith(ext) for ext in exts):
                    continue

                abs_path = Path(root).joinpath(file).as_posix()
                rel_to_resolved = os.path.relpath(abs_path, resolved_dir).replace('\\', '/')

                if rel_to_resolved == '.':
                    yield directory_path
                elif not directory_path:
                    yield rel_to_resolved
                else:
                    sep = "" if directory_path.endswith('/') else "/"
                    yield f"{directory_path}{sep}{rel_to_resolved}"