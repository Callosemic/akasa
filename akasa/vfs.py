import os
import stat
import contextvars
import time
import json
import shutil
import queue
import hashlib
import traceback
import threading
from collections import OrderedDict
from pathlib import Path, PurePosixPath
from typing import Optional, Dict, List, Tuple, Any, Callable, Set, Union
from akasa.context import parse_uri, is_subpath, atomic_write_text, get_current_origin
from akasa.hooks import hooks
from akasa.db import get_connection, _get_tenant_control_dir as get_tenant_control_dir, _get_all_workspace_ids as get_all_workspace_ids
from akasa.uri import AkasaURI
from akasa.merge import three_way_merge_text

_ARTIFACT_PATH_RESOLVER: Optional[Callable[[str, Optional[str]], str]] = None
_PROTECTED_VFS_TARGETS: Set[Tuple[str, str]] = set()


def register_protected_vfs_targets(*targets: Union[str, Tuple[str, str]]) -> None:
    """DI Interface: Host OS registers protected (namespace, basename) pairs that VFS mutations must refuse."""
    for item in targets:
        if isinstance(item, tuple) and len(item) == 2:
            ns, base = str(item[0]).strip().lower(), str(item[1]).strip().lower()
            if base:
                _PROTECTED_VFS_TARGETS.add((ns, base))
        elif isinstance(item, str) and item.strip():
            norm = item.replace("\\", "/").strip().strip("/").lower()
            if "/" in norm:
                ns, _, base = norm.rpartition("/")
                _PROTECTED_VFS_TARGETS.add((ns, base))
            else:
                _PROTECTED_VFS_TARGETS.add(("", norm))


def set_vfs_artifact_path_resolver(resolver_fn: Callable[[str, Optional[str]], str]) -> None:
    """DI Interface: Ring 2 state module binds resolve_system_artifact_path so vfs.py has zero upward dependencies."""
    global _ARTIFACT_PATH_RESOLVER
    _ARTIFACT_PATH_RESOLVER = resolver_fn


_VFS_READ_CACHE = OrderedDict()
_VFS_READ_CACHE_BYTES = 0
_VFS_READ_CACHE_MAX_BYTES = 8 * 1024 * 1024  # 8 MB RAM cache
_VFS_READ_CACHE_MAX_FILE_BYTES = 1 * 1024 * 1024  # Only cache individual files <= 1 MB
_VFS_CACHE_LOCK = threading.Lock()

_VFS_TOMBSTONE = object()
_VFS_PENDING_WRITES: Dict[str, Tuple[int, Any]] = {}
_VFS_PENDING_SEQ = 0


def _stage_pending_write(resolved_path: Optional[str], content_or_tombstone: Any) -> int:
    """Synchronously stages a pending write or tombstone in RAM before queuing disk I/O."""
    global _VFS_PENDING_SEQ
    if not resolved_path:
        return 0
    norm_res = Path(resolved_path).as_posix()
    with _VFS_CACHE_LOCK:
        _VFS_PENDING_SEQ += 1
        seq = _VFS_PENDING_SEQ
        _VFS_PENDING_WRITES[norm_res] = (seq, content_or_tombstone)
        if content_or_tombstone is _VFS_TOMBSTONE:
            dir_prefix = norm_res.rstrip('/') + '/'
            for k in list(_VFS_PENDING_WRITES.keys()):
                if k.startswith(dir_prefix):
                    _VFS_PENDING_WRITES[k] = (seq, _VFS_TOMBSTONE)
        return seq


def _clear_pending_write(resolved_path: Optional[str], seq: Optional[int] = None) -> None:
    """Clears a staged pending write once physical disk commit completes, unless superseded by a newer write."""
    if not resolved_path:
        return
    norm_res = Path(resolved_path).as_posix()
    with _VFS_CACHE_LOCK:
        cur = _VFS_PENDING_WRITES.get(norm_res)
        if cur is not None and (seq is None or cur[0] <= seq):
            _VFS_PENDING_WRITES.pop(norm_res, None)
        if seq is not None:
            dir_prefix = norm_res.rstrip('/') + '/'
            for k, val in list(_VFS_PENDING_WRITES.items()):
                if k.startswith(dir_prefix) and val[0] <= seq:
                    _VFS_PENDING_WRITES.pop(k, None)
def has_pending_vfs_write(resolved_path: Optional[str]) -> bool:
    """Returns True if resolved_path currently has an un-flushed write in _VFS_PENDING_WRITES."""
    if not resolved_path:
        return False
    norm_res = Path(resolved_path).as_posix()
    with _VFS_CACHE_LOCK:
        return norm_res in _VFS_PENDING_WRITES
def read_cached_physical_text(resolved_path: str, bypass_read_cache: bool = False) -> Tuple[Optional[int], Optional[str]]:
    """
    Unified Ring 2 Read-Your-Writes primitive:
    Inspects _VFS_PENDING_WRITES first (returning (-seq, content)), then _VFS_READ_CACHE
    (verifying st_mtime_ns and st_size), and finally falls back to reading physical disk.
    Returns (revision_token, text) or (None, None) if missing/deleted.
    """
    if not resolved_path:
        return None, None
    norm_res = Path(resolved_path).as_posix()
    with _VFS_CACHE_LOCK:
        pending = _VFS_PENDING_WRITES.get(norm_res)
        if pending is not None:
            if pending[1] is _VFS_TOMBSTONE:
                return None, None
            return -int(pending[0]), str(pending[1])

    try:
        st = os.stat(norm_res)
    except OSError:
        with _VFS_CACHE_LOCK:
            if norm_res in _VFS_PENDING_WRITES:
                p_retry = _VFS_PENDING_WRITES[norm_res]
                if p_retry[1] is not _VFS_TOMBSTONE:
                    return -int(p_retry[0]), str(p_retry[1])
        return None, None

    if stat.S_ISDIR(st.st_mode):
        return None, None

    if not bypass_read_cache:
        cached = _cache_get(norm_res, st=st)
        if cached is not None:
            return int(st.st_mtime_ns), cached

    try:
        with open(norm_res, 'r', encoding='utf-8') as f:
            text = f.read()
        _cache_put(norm_res, text, st=st)
        return int(st.st_mtime_ns), text
    except (UnicodeDecodeError, OSError):
        return None, None


def drain_write_queue(timeout: float = 5.0) -> None:
    """Blocks until every queued VFS write has reached disk; on failure, logs and returns (test-harness use)."""
    try:
        sync_vfs_barrier("default", timeout=timeout)
    except (RuntimeError, TimeoutError) as e:
        print(f"⚠️ [VFS] Could not drain the write queue: {e}")


def reset() -> None:
    """
    Drains the async write queue, then purges the VFS read cache, pending write overlay, and ancestor
    ring buffer for isolated test harnesses. Draining first matters: a write queued under one daemon
    cwd must not execute after the harness switches cwd, or it lands at the wrong physical path.
    """
    global _VFS_READ_CACHE_BYTES
    drain_write_queue()
    with _VFS_CACHE_LOCK:
        _VFS_READ_CACHE.clear()
        _VFS_READ_CACHE_BYTES = 0
        _VFS_PENDING_WRITES.clear()
    with _ANCESTOR_LOCK:
        _ANCESTOR_CACHE.clear()


_ANCESTOR_CACHE: Dict[str, "OrderedDict[str, str]"] = OrderedDict()
_ANCESTOR_LOCK = threading.Lock()
_MAX_ANCESTORS_PER_FILE = 12
_MAX_ANCESTOR_FILE_BYTES = 1 * 1024 * 1024  # 1 MB cap


def compute_vfs_sha256(text: str) -> str:
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


def record_vfs_ancestor(workspace_id: str, filepath_or_resolved: str, text: str, text_hash: Optional[str] = None) -> str:
    if text is None:
        return ""
    h = text_hash or compute_vfs_sha256(text)
    if len(text.encode('utf-8')) > _MAX_ANCESTOR_FILE_BYTES:
        return h
    resolved = (
        filepath_or_resolved
        if os.path.isabs(str(filepath_or_resolved))
        else (_resolve_physical_path(filepath_or_resolved, workspace_id or "default") or str(filepath_or_resolved))
    )
    with _ANCESTOR_LOCK:
        ring = _ANCESTOR_CACHE.get(resolved)
        if ring is None:
            ring = OrderedDict()
            _ANCESTOR_CACHE[resolved] = ring
        ring[h] = text
        ring.move_to_end(h)
        _ANCESTOR_CACHE.move_to_end(resolved)
        while len(ring) > _MAX_ANCESTORS_PER_FILE:
            ring.popitem(last=False)
        while len(_ANCESTOR_CACHE) > 512:
            _ANCESTOR_CACHE.popitem(last=False)
    return h


def lookup_vfs_ancestor(workspace_id: str, filepath_or_resolved: str, base_hash: Optional[str], fallback_base_content: Optional[str] = None) -> Optional[str]:
    if not base_hash:
        return fallback_base_content
    resolved = (
        filepath_or_resolved
        if os.path.isabs(str(filepath_or_resolved))
        else (_resolve_physical_path(filepath_or_resolved, workspace_id or "default") or str(filepath_or_resolved))
    )
    with _ANCESTOR_LOCK:
        ring = _ANCESTOR_CACHE.get(resolved)
        if ring and base_hash in ring:
            return ring[base_hash]
    if isinstance(fallback_base_content, str):
        return fallback_base_content
    return None
def _relocate_vfs_ancestors(resolved_src: str, resolved_dest: str):
    if not resolved_src or not resolved_dest:
        return
    with _ANCESTOR_LOCK:
        ring = _ANCESTOR_CACHE.pop(resolved_src, None)
        if ring is not None:
            _ANCESTOR_CACHE[resolved_dest] = ring
            _ANCESTOR_CACHE.move_to_end(resolved_dest)



def get_vfs_cache_stats() -> dict:
    """Returns public telemetry statistics for the VFS read cache and write queue."""
    with _VFS_CACHE_LOCK:
        files_count = len(_VFS_READ_CACHE)
        mb_used = round(_VFS_READ_CACHE_BYTES / (1024 * 1024), 2)
        pending_count = len(_VFS_PENDING_WRITES)
    return {
        "vfs_read_cache_files": files_count,
        "vfs_read_cache_mb": mb_used,
        "vfs_pending_overlay_files": pending_count,
        "vfs_write_queue_depth": _VFS_WRITE_QUEUE.qsize()
    }


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
    record_vfs_ancestor("default", resolved_path, content)


def evict_vfs_cache(resolved_path: str, preserve_ancestors: bool = False):
    global _VFS_READ_CACHE_BYTES
    if not resolved_path:
        return
    dir_prefix = resolved_path.rstrip('/') + '/'
    with _VFS_CACHE_LOCK:
        old = _VFS_READ_CACHE.pop(resolved_path, None)
        if old:
            _VFS_READ_CACHE_BYTES -= old[2]
        child_keys = [k for k in _VFS_READ_CACHE.keys() if k.startswith(dir_prefix)]
        for k in child_keys:
            entry = _VFS_READ_CACHE.pop(k, None)
            if entry:
                _VFS_READ_CACHE_BYTES -= entry[2]
    if not preserve_ancestors:
        with _ANCESTOR_LOCK:
            _ANCESTOR_CACHE.pop(resolved_path, None)
            anc_children = [k for k in _ANCESTOR_CACHE.keys() if k.startswith(dir_prefix)]
            for k in anc_children:
                _ANCESTOR_CACHE.pop(k, None)

_cache_evict = evict_vfs_cache
def _is_file_identical(resolved_path: str, content: str, ignore_pending: bool = False) -> bool:
    """Fast-path idempotency check: pending overlay + single stat + RAM cache check before touching disk."""
    if not resolved_path:
        return False
    norm_res = Path(resolved_path).as_posix()
    if not ignore_pending:
        with _VFS_CACHE_LOCK:
            pending = _VFS_PENDING_WRITES.get(norm_res)
            if pending is not None:
                if pending[1] is _VFS_TOMBSTONE:
                    return False
                return pending[1] == content
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
        uri_parts = uri_obj.parts
        domain = uri_parts[0] if uri_parts else ""

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
        if _ARTIFACT_PATH_RESOLVER is not None:
            return _ARTIFACT_PATH_RESOLVER(filepath, workspace_id)
        return Path(filepath).resolve().as_posix()

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


def resolve_physical_path(filepath, workspace_id="default", is_absolute_artifact=False):
    """Public SSOT gateway for resolving a canonical URI to its physical host path."""
    return _resolve_physical_path(filepath, workspace_id, is_absolute_artifact=is_absolute_artifact)


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
    hooks.sync_event_bus_barrier(timeout=timeout)


def _execute_single_vfs_op(workspace_id: str, op_type: str, filepath: str, content: str, op_data: dict, ledger_rows: list, emitted_mutations: list) -> None:
    """Executes a single physical VFS operation ('save', 'delete', or 'move') and appends ledger/mutation records."""
    now_ts = time.time()
    p_seq = op_data.get("_pending_seq")
    ignore_ledger = bool(op_data.get("is_absolute_artifact") or op_data.get("ignore_ledger"))

    if op_type == "delete" and filepath:
        resolved_path = _resolve_physical_path(filepath, workspace_id, op_data.get("is_absolute_artifact"))
        try:
            hooks.emit('pre_file_save', workspace_id=workspace_id, filepath=filepath, resolved_path=resolved_path, content="", data=op_data)
        except Exception:
            pass
        if resolved_path and os.path.exists(resolved_path):
            _cache_evict(resolved_path)
            if os.path.isdir(resolved_path):
                shutil.rmtree(resolved_path)
            else:
                os.remove(resolved_path)
        _clear_pending_write(resolved_path, p_seq)
        if not ignore_ledger:
            ledger_rows.append((filepath, 'deleted', now_ts))
        emitted_mutations.append({"filepath": filepath, "operation": "delete", "ignore_ledger": ignore_ledger})

    elif op_type == "move" and filepath:
        dest_path = op_data.get("dest_path", "")
        resolved_src = _resolve_physical_path(filepath, workspace_id, op_data.get("is_absolute_artifact"))
        resolved_dest = _resolve_physical_path(dest_path, workspace_id, op_data.get("is_absolute_artifact"))
        try:
            hooks.emit('pre_file_save', workspace_id=workspace_id, filepath=filepath, resolved_path=resolved_src, content="", data=op_data)
            hooks.emit('pre_file_save', workspace_id=workspace_id, filepath=dest_path, resolved_path=resolved_dest, content="", data=op_data)
        except Exception:
            pass
        if resolved_src and resolved_dest and os.path.exists(resolved_src):
            _relocate_vfs_ancestors(resolved_src, resolved_dest)
            _cache_evict(resolved_src, preserve_ancestors=True)
            _cache_evict(resolved_dest, preserve_ancestors=True)
            os.makedirs(Path(resolved_dest).parent, exist_ok=True)
            shutil.move(resolved_src, resolved_dest)
            if not ignore_ledger:
                ledger_rows.append((filepath, 'deleted', now_ts))
                ledger_rows.append((dest_path, 'added', now_ts))
            emitted_mutations.append({"filepath": filepath, "operation": "delete", "relocated_to": dest_path, "ignore_ledger": ignore_ledger})
            emitted_mutations.append({"filepath": dest_path, "operation": "save", "relocated_from": filepath, "ignore_ledger": ignore_ledger})
        else:
            print(f"❌ [VFS Pipeline] Move aborted: Source file missing on disk ({filepath})")
        _clear_pending_write(resolved_src, p_seq)
        _clear_pending_write(resolved_dest, p_seq)

    elif filepath:
        save_res = execute_vfs_save_physical(workspace_id, filepath, content, op_data, defer_events=True)
        if save_res:
            s_rows, s_muts = save_res
            ledger_rows.extend(s_rows)
            emitted_mutations.extend(s_muts)


def _vfs_commit_worker():
    """Consumes write payloads in micro-batches off-thread to eliminate SQLite commit thrashing."""
    while True:
        if _VFS_SHUTDOWN_SIGNAL.is_set() and _VFS_WRITE_QUEUE.empty():
            break
        try:
            first_task = _VFS_WRITE_QUEUE.get(timeout=1.0)
            if first_task is None:
                _VFS_WRITE_QUEUE.task_done()
                continue

            # Brief 5ms coalescing window for non-barrier writes so tight producer loops
            # accumulate in _VFS_WRITE_QUEUE before we drain the slice.
            first_data = first_task[3] if isinstance(first_task[3], dict) else {}
            if first_data.get("action") != "barrier" and _VFS_WRITE_QUEUE.empty():
                time.sleep(0.005)

            batch = [first_task]
            while len(batch) < 64:
                if batch[-1] is not None and isinstance(batch[-1][3], dict) and batch[-1][3].get("action") == "barrier":
                    break
                try:
                    nxt = _VFS_WRITE_QUEUE.get_nowait()
                    if nxt is None:
                        _VFS_WRITE_QUEUE.task_done()
                        break
                    batch.append(nxt)
                except queue.Empty:
                    break

            ws_ledger_rows: Dict[str, List[Tuple[str, str, float]]] = {}
            ws_mutations: Dict[str, List[Dict[str, Any]]] = {}
            barriers_to_release = []

            for task in batch:
                workspace_id, filepath, content, data = task
                try:
                    action = data.get("action", "save")
                    if action == "barrier":
                        if "barrier_event" in data:
                            barriers_to_release.append(data["barrier_event"])
                        continue
                    elif action == "bulk":
                        ledger_rows = ws_ledger_rows.setdefault(workspace_id, [])
                        emitted_mutations = ws_mutations.setdefault(workspace_id, [])
                        for op_item in data.get("operations", []):
                            _execute_single_vfs_op(
                                workspace_id,
                                op_item.get("action", "save"),
                                op_item.get("filepath", ""),
                                op_item.get("content", ""),
                                op_item,
                                ledger_rows,
                                emitted_mutations,
                            )
                    else:
                        ledger_rows = ws_ledger_rows.setdefault(workspace_id, [])
                        emitted_mutations = ws_mutations.setdefault(workspace_id, [])
                        _execute_single_vfs_op(
                            workspace_id,
                            action,
                            filepath,
                            content,
                            data,
                            ledger_rows,
                            emitted_mutations,
                        )
                except Exception as e:
                    print(f"❌ [VFS Pipeline] Background operation failed for {filepath}: {e}")
                    try:
                        hooks.emit_background('system_error', source="VFS Pipeline", error_type=type(e).__name__, message=str(e), traceback=traceback.format_exc(), payload=json.dumps(data) if isinstance(data, dict) else json.dumps({"data": str(data)}), workspace_id=workspace_id)
                    except Exception: pass

            for ws_id, rows in ws_ledger_rows.items():
                if rows:
                    try:
                        db_conn = get_connection("workers", workspace_id=ws_id)
                        db_conn.executemany(
                            "INSERT OR REPLACE INTO vfs_event_log (filepath, mutation_type, timestamp) VALUES (?, ?, ?)",
                            rows
                        )
                        db_conn.commit()
                    except Exception as db_err:
                        print(f"❌ [VFS Pipeline] Ledger batch commit failed for workspace {ws_id}: {db_err}")
            for ws_id, muts in ws_mutations.items():
                if muts:
                    try:
                        hooks.emit_background('vfs_mutated', workspace_id=ws_id, mutations=muts)
                    except Exception as emit_err:
                        print(f"❌ [VFS Pipeline] vfs_mutated emission failed for workspace {ws_id}: {emit_err}")

            for b_ev in barriers_to_release:
                b_ev.set()

            for _ in batch:
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
_PROTECTED_WRITE_MESSAGE = "Refusing to modify a host-protected file (registered with register_protected_vfs_targets)."


def _protected_segment_match(segments: List[str], ns_segments: List[str], protected_base: str) -> bool:
    """
    True when the path names the protected module or anything that would shadow it at import time:
    the file itself, a same-named package directory (<stem>/__init__.py), compiled or extension
    variants (<stem>.cpython-*.so, <stem>.pyc), and __pycache__ entries. The namespace must be the
    directory the module sits in, compared by whole path segments (so 'pkg' never matches 'pkg-demo').
    """
    stem = protected_base.rsplit(".", 1)[0] if "." in protected_base else protected_base
    for idx, seg in enumerate(segments):
        is_file = idx == len(segments) - 1
        hit = seg == protected_base or (seg == stem and not is_file) or seg.startswith(stem + ".")
        if not hit:
            continue
        parents = segments[:idx]
        if parents and parents[-1] == "__pycache__":
            parents = parents[:-1]
        if not ns_segments or parents[-len(ns_segments):] == ns_segments:
            return True
    return False


def _is_s8_protected_target(filepath: Optional[str], resolved_path: Optional[str] = None) -> bool:
    """Refuses writes, moves, or deletions that would replace or shadow a host-registered protected file."""
    if not _PROTECTED_VFS_TARGETS:
        return False
    for candidate in (filepath, resolved_path):
        if not candidate:
            continue
        norm = str(candidate).replace("\\", "/").lower()
        if "://" in norm:
            norm = norm.partition("://")[2]
        segments = [seg for seg in PurePosixPath(norm).parts if seg not in ("/", ".")]
        for ns_token, protected_base in _PROTECTED_VFS_TARGETS:
            ns_segments = [seg for seg in PurePosixPath(ns_token).parts if seg not in ("/", ".")]
            if _protected_segment_match(segments, ns_segments, protected_base):
                return True
    return False


# Host policy for mutations that touch a protected target (S8). Without a handler they are refused.
_PROTECTED_WRITE_HANDLER: Optional[Callable[[Dict[str, Any]], Any]] = None
# Set only inside execute_confirmed_protected_write, for the duration of that one call.
_CONFIRMED_PROTECTED_WRITE: contextvars.ContextVar[bool] = contextvars.ContextVar("akasa_confirmed_protected_write", default=False)


def set_protected_write_handler(handler: Optional[Callable[[Dict[str, Any]], Any]]) -> None:
    """
    DI Interface: the host decides what happens to a VFS mutation that touches a protected target.

    The handler is called as handler(request) with request = {"action": "save" | "delete" | "move",
    "workspace_id", "filepath", "dest_path", "content", "data", "origin"}, where origin is the active execution
    frame's origin. It returns (body, status), or a body alone (status 202), and that is what the caller gets back;
    nothing is written. Passing None restores the default, which refuses every such mutation.
    """
    global _PROTECTED_WRITE_HANDLER
    _PROTECTED_WRITE_HANDLER = handler


def is_protected_vfs_target(filepath: Optional[str], workspace_id: Optional[str] = None, is_absolute_artifact: bool = False) -> bool:
    """
    True when filepath (or, given a workspace, the physical path it resolves to) is a host-protected target or
    would shadow one. A pure check: hosts use it to tell a person ahead of time that a change will be held.
    """
    if not filepath:
        return False
    resolved = _resolve_physical_path(filepath, workspace_id, is_absolute_artifact) if workspace_id else None
    return _is_s8_protected_target(filepath, resolved)


def _protected_hit(*paths: Optional[str]) -> bool:
    """True when any path touches a protected target, unless a confirmed protected write is in progress."""
    if _CONFIRMED_PROTECTED_WRITE.get():
        return False
    return any(_is_s8_protected_target(p) for p in paths if p)


def _route_protected_mutation(
    action: str,
    workspace_id: str,
    filepath: str,
    dest_path: Optional[str] = None,
    content: Optional[str] = None,
    data: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, Any], int]:
    """Hands a protected mutation to the host handler, or refuses it when none is registered."""
    if _PROTECTED_WRITE_HANDLER is None:
        print(f"🛑 [VFS Guarded Write] Refused {action} of protected file '{filepath}'.")
        return {"status": "error", "message": _PROTECTED_WRITE_MESSAGE}, 403
    request = {
        "action": action,
        "workspace_id": workspace_id,
        "filepath": filepath,
        "dest_path": dest_path,
        "content": content,
        "data": dict(data or {}),
        "origin": get_current_origin(),
    }
    try:
        result = _PROTECTED_WRITE_HANDLER(request)
    except Exception as exc:
        print(f"❌ [VFS Guarded Write] Protected write handler failed for '{filepath}': {exc}")
        return {"status": "error", "message": _PROTECTED_WRITE_MESSAGE}, 403
    if isinstance(result, tuple) and len(result) == 2 and isinstance(result[1], int):
        return result[0], result[1]
    return result, 202


def execute_confirmed_protected_write(
    workspace_id: str,
    filepath: str,
    content: Optional[str] = None,
    data: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, Any], int]:
    """
    Writes content to a protected target, or deletes it when content is None, skipping the protected-target check
    for this one call. Every other guard still applies (read-only mounts, volume resolution, the write queue).
    Hosts call this only from their human confirmation path, after a person approved this exact change.
    """
    token = _CONFIRMED_PROTECTED_WRITE.set(True)
    try:
        if content is None:
            # The save path deletes too, and unlike execute_vfs_delete it honours is_absolute_artifact.
            body = execute_vfs_save(workspace_id, filepath, "", dict(data or {}, action="delete"))
        else:
            body = execute_vfs_save(workspace_id, filepath, content, data)
        return body, (202 if isinstance(body, dict) and body.get("status") == "accepted" else 400)
    finally:
        _CONFIRMED_PROTECTED_WRITE.reset(token)


def execute_vfs_move(workspace_id, filepath, dest_path):
    if str(filepath).startswith("ws://") or str(dest_path).startswith("ws://"):
        return {"status": "error", "message": "Cross-workspace virtual mounts ('ws://') are strictly read-only."}, 403
    if _protected_hit(filepath, dest_path):
        return _route_protected_mutation("move", workspace_id, filepath, dest_path=dest_path)
    resolved_src = _resolve_physical_path(filepath, workspace_id)
    resolved_dest = _resolve_physical_path(dest_path, workspace_id)
    src_text = None
    if resolved_src:
        with VFSTransaction(workspace_id) as vfs:
            src_text = vfs.read(filepath)
    seq = _stage_pending_write(resolved_src, _VFS_TOMBSTONE)
    if resolved_dest and src_text is not None:
        seq = _stage_pending_write(resolved_dest, src_text)
    _ensure_vfs_worker_alive()
    _VFS_WRITE_QUEUE.put((workspace_id, filepath, "", {"action": "move", "dest_path": dest_path, "_pending_seq": seq}))
    return {"status": "accepted", "message": f"File move queued."}, 202
def execute_vfs_delete(workspace_id, filepath):
    if str(filepath).startswith("ws://"):
        return {"status": "error", "message": "Cross-workspace virtual mounts ('ws://') are strictly read-only."}, 403
    resolved_path = _resolve_physical_path(filepath, workspace_id)
    if _protected_hit(filepath, resolved_path):
        return _route_protected_mutation("delete", workspace_id, filepath)
    seq = _stage_pending_write(resolved_path, _VFS_TOMBSTONE)
    _ensure_vfs_worker_alive()
    _VFS_WRITE_QUEUE.put((workspace_id, filepath, "", {"action": "delete", "_pending_seq": seq}))
    return {"status": "accepted", "message": f"File deletion queued."}, 202

def execute_vfs_bulk(workspace_id, operations):
    sanitized = []
    protected_results = []
    for op in operations:
        fp = op.get("filepath", "")
        dest = op.get("dest_path", "")
        if str(fp).startswith("ws://") or str(dest).startswith("ws://"):
            continue
        if _protected_hit(fp, dest):
            body, status = _route_protected_mutation(
                op.get("action", "save"), workspace_id, fp, dest_path=dest or None, content=op.get("content"), data=op,
            )
            protected_results.append({"filepath": fp, "status_code": status, **(body if isinstance(body, dict) else {"result": body})})
            continue
        op_copy = dict(op)
        act = op_copy.get("action", "save")
        is_abs = op_copy.get("is_absolute_artifact")
        if act == "delete" and fp:
            res_p = _resolve_physical_path(fp, workspace_id, is_abs)
            op_copy["_pending_seq"] = _stage_pending_write(res_p, _VFS_TOMBSTONE)
        elif act == "move" and fp and dest:
            res_s = _resolve_physical_path(fp, workspace_id, is_abs)
            res_d = _resolve_physical_path(dest, workspace_id, is_abs)
            src_text = None
            if res_s:
                with VFSTransaction(workspace_id) as vfs:
                    src_text = vfs.read(fp, is_absolute_artifact=bool(is_abs))
            seq = _stage_pending_write(res_s, _VFS_TOMBSTONE)
            if res_d and src_text is not None:
                seq = _stage_pending_write(res_d, src_text)
            op_copy["_pending_seq"] = seq
        elif act == "save" and fp:
            res_p = _resolve_physical_path(fp, workspace_id, is_abs)
            op_copy["_pending_seq"] = _stage_pending_write(res_p, op_copy.get("content", ""))
        sanitized.append(op_copy)
    _ensure_vfs_worker_alive()
    _VFS_WRITE_QUEUE.put((workspace_id, "", "", {"action": "bulk", "operations": sanitized}))
    result = {"status": "accepted", "message": f"Bulk operation ({len(sanitized)} item(s)) queued.", "operations": sanitized}
    if protected_results:
        result["protected"] = protected_results
    return result, 202
def execute_vfs_save(workspace_id, filepath, content, data=None):
    data = dict(data or {})
    if str(filepath).startswith("ws://"):
        print(f"🛑 [VFS Security] Write dropped: Virtual mount '{filepath}' is strictly read-only.")
        return {"status": "error", "message": "Cross-workspace virtual mounts ('ws://') are strictly read-only."}
    resolved_path = _resolve_physical_path(filepath, workspace_id, data.get("is_absolute_artifact"))
    if _protected_hit(filepath, resolved_path, data.get("delete_source")):
        body, _status = _route_protected_mutation(
            "delete" if data.get("action") == "delete" else "save", workspace_id, filepath, content=content, data=data,
        )
        return body
    # Pre-flight Volume Validation
    if not data.get("is_absolute_artifact"):
        if not resolved_path:
            print(f"⚠️ [VFS] Save dropped: Volume for '{filepath}' is not mounted.")
            return {"status": "error", "message": f"Volume for '{filepath}' is not mounted."}
    action = data.get("action", "save")
    if action == "delete":
        data["_pending_seq"] = _stage_pending_write(resolved_path, _VFS_TOMBSTONE)
    elif action == "save":
        if not data.get("delete_source") and resolved_path and _is_file_identical(resolved_path, content):
            return {"status": "accepted", "message": f"File {filepath} unchanged (idempotent)."}
        if data.get("delete_source") and data["delete_source"] != filepath:
            old_res = _resolve_physical_path(data["delete_source"], workspace_id, data.get("is_absolute_artifact"))
            _stage_pending_write(old_res, _VFS_TOMBSTONE)
        data["_pending_seq"] = _stage_pending_write(resolved_path, content)
        record_vfs_ancestor(workspace_id, resolved_path or filepath, content)

    _ensure_vfs_worker_alive()
    _VFS_WRITE_QUEUE.put((workspace_id, filepath, content, data))
    return {"status": "accepted", "message": f"File {filepath} queued for atomic background commit."}
def execute_vfs_save_physical(workspace_id, filepath, content, data, defer_events=False):
    resolved_path = _resolve_physical_path(filepath, workspace_id, data.get("is_absolute_artifact"))
    p_seq = data.get("_pending_seq") if isinstance(data, dict) else None

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
        if _is_file_identical(resolved_path, content, ignore_pending=True):
            _clear_pending_write(resolved_path, p_seq)
            if os.environ.get("AKASA_DEBUG") == "1":
                print(f"💽 [VFS TELEMETRY] Silent drop (idempotent): {filepath}")
            return None

    if os.environ.get("AKASA_DEBUG") == "1":
        print(f"💽 [VFS TELEMETRY] Writing to disk: {filepath}")
    try:
        atomic_write_text(resolved_path, content, encoding='utf-8')
        _cache_put(resolved_path, content)
    finally:
        _clear_pending_write(resolved_path, p_seq)

    saved_hash = compute_vfs_sha256(content)
    now_ts = time.time()
    ignore_ledger = bool(data.get("is_absolute_artifact") or data.get("ignore_ledger"))
    ledger_rows = []
    emitted_mutations = []

    if not ignore_ledger:
        ledger_rows.append((filepath, 'modified' if not is_new else 'added', now_ts))
    delete_source = data.get("delete_source")
    relocated_from = None
    if delete_source and delete_source != filepath:
        old_abs_path = _resolve_physical_path(delete_source, workspace_id, data.get("is_absolute_artifact"))
        _clear_pending_write(old_abs_path, p_seq)
        if old_abs_path and os.path.exists(old_abs_path) and os.path.abspath(old_abs_path) != os.path.abspath(resolved_path):
            _relocate_vfs_ancestors(old_abs_path, resolved_path)
            _cache_evict(old_abs_path)
            os.remove(old_abs_path)
            relocated_from = delete_source
            emitted_mutations.append({
                "filepath": delete_source,
                "operation": "delete",
                "relocated_to": filepath,
                "ignore_ledger": ignore_ledger
            })
            if not ignore_ledger:
                ledger_rows.append((delete_source, 'deleted', now_ts))

    save_mut = {"filepath": filepath, "operation": "save", "base_hash": saved_hash, "ignore_ledger": ignore_ledger}
    if relocated_from:
        save_mut["relocated_from"] = relocated_from
    emitted_mutations.append(save_mut)

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
    hooks.emit_background('vfs_mutated', workspace_id=workspace_id, mutations=emitted_mutations)
    return None
class VFSTransaction:
    """
    Provides dual-mode immediate queue dispatch and atomic buffered context-manager transactions for VFS mutations.

    A mutation of a protected target is never buffered: it goes to the host's protected-write handler at once and the
    caller gets the handler's answer. It writes nothing, so it does not weaken the transaction's atomicity, and it is
    not withdrawn if the transaction later fails (the host keeps it, for example as a pending change).
    """
    def __init__(self, workspace_id):
        self.workspace_id = workspace_id
        self._buffer = []
        self._in_transaction = False

    def save(self, filepath, content, data=None):
        data = dict(data or {})
        if not data.get("is_absolute_artifact") and filepath and "://" not in str(filepath):
            raise ValueError(f"Non-canonical URI rejected by VFS save (missing scheme:// prefix): '{filepath}'")
        resolved = _resolve_physical_path(filepath, self.workspace_id, data.get("is_absolute_artifact"))
        if not data.get("is_absolute_artifact") and not resolved:
            print(f"⚠️ [VFS] Transaction dropped: Volume for '{filepath}' is not mounted.")
            return {"status": "error", "message": f"Volume for '{filepath}' is not mounted."}
        if self._in_transaction and not _protected_hit(filepath, resolved, data.get("delete_source")):
            self._buffer.append((filepath, content, data))
            return {"status": "buffered", "message": f"File {filepath} buffered in transaction."}
        # A protected save is handed to the host at once, so the caller sees its answer (see _route_protected_mutation).
        # Pre-flight Idempotency Gatekeeper: fast-path size + RAM cache check
        if not data.get("delete_source") and resolved and _is_file_identical(resolved, content):
            return {"status": "accepted", "message": f"File {filepath} unchanged (idempotent)."}
        return execute_vfs_save(self.workspace_id, filepath, content, data)

    def delete(self, filepath, data=None):
        """Ergonomic wrapper for queuing an atomic VFS deletion."""
        data = dict(data or {})
        if not data.get("is_absolute_artifact") and filepath and "://" not in str(filepath):
            raise ValueError(f"Non-canonical URI rejected by VFS delete (missing scheme:// prefix): '{filepath}'")
        data["action"] = "delete"
        resolved = _resolve_physical_path(filepath, self.workspace_id, data.get("is_absolute_artifact"))
        if self._in_transaction and _protected_hit(filepath, resolved):
            return _route_protected_mutation("delete", self.workspace_id, filepath, data=data)
        if self._in_transaction:
            self._buffer.append((filepath, "", data))
            return {"status": "buffered", "message": "File deletion buffered."}, 202
        if data.get("ignore_ledger") or data.get("is_absolute_artifact"):
            res = execute_vfs_save(self.workspace_id, filepath, "", data)
            return res, 202
        return execute_vfs_delete(self.workspace_id, filepath)

    def move(self, filepath, dest_path, data=None):
        """Ergonomic wrapper for queuing an atomic VFS move."""
        data = dict(data or {})
        if not data.get("is_absolute_artifact") and (
            (filepath and "://" not in str(filepath)) or (dest_path and "://" not in str(dest_path))
        ):
            raise ValueError(f"Non-canonical URI rejected by VFS move (missing scheme:// prefix): '{filepath}' -> '{dest_path}'")
        data["action"] = "move"
        data["dest_path"] = dest_path
        if self._in_transaction and _protected_hit(filepath, dest_path):
            return _route_protected_mutation("move", self.workspace_id, filepath, dest_path=dest_path, data=data)
        if self._in_transaction:
            self._buffer.append((filepath, "", data))
            return {"status": "buffered", "message": "File move buffered."}, 202
        return execute_vfs_move(self.workspace_id, filepath, dest_path)

    def bulk(self, operations: List[Dict[str, Any]]):
        """Queues a batch of VFS operations (move, delete, save) in a single atomic commit."""
        if self._in_transaction:
            protected_results = []
            buffered = []
            for op in (operations or []):
                fp = op.get("filepath", "")
                dest = op.get("dest_path", "")
                content = op.get("content", "")
                if _protected_hit(fp, dest):
                    body, status = _route_protected_mutation(
                        op.get("action", "save"), self.workspace_id, fp, dest_path=dest or None, content=content, data=op,
                    )
                    protected_results.append({"filepath": fp, "status_code": status, **(body if isinstance(body, dict) else {"result": body})})
                    continue
                op_copy = dict(op)
                self._buffer.append((fp, content, op_copy))
                buffered.append(op)
            result = {"status": "buffered", "operations": buffered}
            if protected_results:
                result["protected"] = protected_results
            return result, 202
        return execute_vfs_bulk(self.workspace_id, operations)

    def evict_cache(self, resolved_path: str, preserve_ancestors: bool = False) -> None:
        evict_vfs_cache(resolved_path, preserve_ancestors=preserve_ancestors)

    def compute_sha256(self, text: str) -> str:
        return compute_vfs_sha256(text)

    def record_ancestor(self, filepath: str, text: str, text_hash: Optional[str] = None) -> str:
        return record_vfs_ancestor(self.workspace_id, filepath, text, text_hash=text_hash)

    def lookup_ancestor(self, filepath: str, base_hash: Optional[str], fallback_base_content: Optional[str] = None) -> Optional[str]:
        return lookup_vfs_ancestor(self.workspace_id, filepath, base_hash, fallback_base_content=fallback_base_content)

    def three_way_merge(self, base_text: str, disk_text: str, incoming_text: str) -> Tuple[bool, str]:
        return three_way_merge_text(base_text, disk_text, incoming_text)
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
        _, text = read_cached_physical_text(resolved)
        return text

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
            ignored_dirs = hooks.emit_first('request_vfs_ignore_dirs', workspace_id=self.workspace_id, default=[])
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