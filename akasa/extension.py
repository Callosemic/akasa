import os
import uuid
import json
import time
import shutil
import inspect
import traceback
import threading
import functools
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Dict, Any, List, TypedDict
from flask import Blueprint, request, jsonify

if TYPE_CHECKING:
    from akasa.types import WorkspaceID, FilePath, VFSStorageEngine, WorkspacePathsDict
from akasa.db import get_connection, register_schema
from akasa.events import sse_bus
from akasa.hooks import hooks
import akasa.utils as _akasa_utils
from akasa.utils import extension_auth, load_config, get_workspace_physics, get_tenant_control_dir, load_json_config, save_json_config, parse_uri, get_active_modules
from akasa.vfs import VFSTransaction, _resolve_physical_path, sync_vfs_barrier as _vfs_sync_barrier

class SettingsUpdateResponse(TypedDict, total=False):
    status: str
    requires_refresh: bool
    job_id: str
    error: str

class JobManager:
    def __init__(self, ext_name, workspace_id, job_id=None):
        self.ext_name = ext_name
        self.workspace_id = workspace_id
        self.current_job_id = job_id

    def submit(self, task_name, coalesce=False, job_category="system_background", job_id=None, **kwargs):
        from akasa.workers import submit_immediate_job as _submit_immediate_job
        job_prefix = self.ext_name[:3].lower()
        resolved_id = job_id or f"{job_prefix}_{uuid.uuid4().hex[:8]}"
        args_json = json.dumps(kwargs)
        return _submit_immediate_job(
            resolved_id, self.ext_name, task_name, args_json, self.workspace_id, coalesce=coalesce, job_category=job_category
        )

    def submit_one_shot(self, task_name, delay_ms, job_id=None, **kwargs):
        from akasa.workers import submit_one_shot_job as _submit_one_shot_job
        kwargs.pop("job_category", None)
        job_prefix = self.ext_name[:3].lower()
        resolved_id = job_id or f"{job_prefix}_{uuid.uuid4().hex[:8]}"
        args_json = json.dumps(kwargs)
        _submit_one_shot_job(resolved_id, self.ext_name, task_name, delay_ms, args_json, self.workspace_id)
        return resolved_id

    def schedule(self, task_name, interval_ms, jitter_ms=0, job_id=None, **kwargs):
        """SDK Helper: Schedules a recurring task in the Metronome ledger."""
        from akasa.workers import submit_job as _submit_job
        kwargs.pop("job_category", None)
        job_prefix = self.ext_name[:3].lower()
        resolved_id = job_id or f"{job_prefix}_{uuid.uuid4().hex[:8]}"
        args_json = json.dumps(kwargs)
        _submit_job(resolved_id, self.ext_name, task_name, interval_ms, args_json, jitter_ms=jitter_ms, workspace_id=self.workspace_id)
        return resolved_id

    def register_ephemeral(self, filepath, ttl_seconds=3600, owner=None):
        """SDK Helper: Registers a temporary file artifact for automatic TTL garbage collection."""
        from akasa.workers import register_ephemeral_artifact as _register_ephemeral_artifact
        _register_ephemeral_artifact(filepath, owner or self.ext_name, ttl_seconds, workspace_id=self.workspace_id)

    def get_ephemerals(self, owner=None) -> List[str]:
        """SDK Helper: Returns all active ephemeral artifact filepaths for the workspace (or specific owner)."""
        conn = get_connection("workers", workspace_id=self.workspace_id)
        if owner:
            rows = conn.execute("SELECT filepath FROM ephemeral_artifacts WHERE module_owner=?", (owner,)).fetchall()
        else:
            rows = conn.execute("SELECT filepath FROM ephemeral_artifacts").fetchall()
        return [r['filepath'] for r in rows]

    def clear_ephemerals(self, owner=None):
        """SDK Helper: Purges ephemeral artifact records from the ledger for a specific owner."""
        conn = get_connection("workers", workspace_id=self.workspace_id)
        target_owner = owner or self.ext_name
        conn.execute("DELETE FROM ephemeral_artifacts WHERE module_owner=?", (target_owner,))
        conn.commit()
    def submit_chain(self, steps, on_complete_hook=None, job_category="system_background", **kwargs):
        """SDK Helper: Automatically constructs and dispatches a parallel-capable DAG background job chain."""
        if not steps: return None
        from akasa.workers import start_dag_chain as _start_dag_chain
        return _start_dag_chain(
            steps,
            self.workspace_id,
            on_complete_hook=on_complete_hook,
            job_category=job_category,
            **kwargs
        )
    def map_parallel(self, fn, items) -> list:
        """SDK Helper: Executes fn(item) across items using the shared bounded kernel I/O pool."""
        from akasa.workers import run_parallel as _run_parallel
        return _run_parallel(fn, items)

    def wait_for(self, prefix: str = None, job_id: str = None, timeout: float = 10.0) -> bool:
        """SDK Helper: Blocks until matching in-flight immediate jobs complete via in-memory threading.Event."""
        from akasa.workers import wait_for_jobs as _wait_for_jobs
        return _wait_for_jobs(self.workspace_id, prefix=prefix, job_id=job_id, timeout=timeout)

    def is_in_flight(self, task_name=None, prefix=None, args_like=None, window_sec=None):
        """SDK Helper: Checks if a specific background worker task (or ID prefix) is currently queued, processing, or within a debounce window."""
        conn = get_connection("workers", workspace_id=self.workspace_id)
        if prefix:
            row = conn.execute(
                "SELECT id FROM immediate_jobs WHERE id LIKE ? AND status IN ('pending', 'processing')",
                (f"{prefix}%",)
            ).fetchone()
            return bool(row)
        if args_like and window_sec is not None:
            now = time.time()
            row = conn.execute(
                """
                SELECT id FROM immediate_jobs 
                WHERE ext_name = ? AND callback_name = ?
                AND ((status IN ('pending', 'processing') AND created_at > ?) OR created_at > ?)
                AND args_json LIKE ?
                """,
                (self.ext_name, task_name, now - 300.0, now - window_sec, args_like)
            ).fetchone()
            return bool(row)
        row = conn.execute(
            "SELECT id FROM immediate_jobs WHERE ext_name=? AND callback_name=? AND status IN ('pending', 'processing')",
            (self.ext_name, task_name)
        ).fetchone()
        return bool(row)

    def find_active_pipeline_job(self, worker_names: List[str], force_full: bool = False, window_sec: float = 300.0) -> Optional[str]:
        """SDK Helper: Checks if any of the given pipeline worker names are actively running and compatible with force_full."""
        if not worker_names:
            return None
        conn = get_connection("workers", workspace_id=self.workspace_id)
        cutoff = time.time() - window_sec
        placeholders = ", ".join(["?"] * len(worker_names))
        existing_job = conn.execute(
            f"SELECT id, args_json FROM immediate_jobs WHERE callback_name IN ({placeholders}) AND status IN ('pending', 'processing') AND updated_at > ?",
            tuple(worker_names) + (cutoff,)
        ).fetchone()
        if existing_job:
            try:
                job_args = json.loads(existing_job['args_json'])
                if job_args.get("force_full") or not force_full:
                    return existing_job['id']
            except Exception:
                pass
        return None

    def get_scheduled_args(self, job_id) -> Optional[Dict[str, Any]]:
        """SDK Helper: Retrieves the parsed args_json dictionary for a pending scheduled/delayed job."""
        conn = get_connection("workers", workspace_id=self.workspace_id)
        row = conn.execute("SELECT args_json FROM jobs WHERE id=? AND status IN ('pending', 'running')", (job_id,)).fetchone()
        if row and row['args_json']:
            try:
                return json.loads(row['args_json'])
            except Exception:
                return None
        return None

    def get_status(self, job_id) -> Optional[Dict[str, Any]]:
        """SDK Helper: Retrieves the current status and artifact of an immediate job."""
        conn = get_connection("workers", workspace_id=self.workspace_id)
        job = conn.execute(
            "SELECT ext_name, status, status_message, artifact_json, created_at, updated_at FROM immediate_jobs WHERE id=?",
            (job_id,)
        ).fetchone()
        if not job:
            return None
        return {
            "id": job_id,
            "ext_name": job['ext_name'],
            "status": job['status'],
            "message": job['status_message'],
            "artifact": json.loads(job['artifact_json']) if job['artifact_json'] else {},
            "created_at": job['created_at'],
            "updated_at": job['updated_at']
        }

    def get_recent_logs(self, limit=20) -> List[Dict[str, Any]]:
        """SDK Helper: Retrieves the most recent immediate job execution logs for this extension."""
        conn = get_connection("workers", workspace_id=self.workspace_id)
        cursor = conn.execute(
            "SELECT id, callback_name, status, status_message, artifact_json, updated_at "
            "FROM immediate_jobs WHERE ext_name = ? ORDER BY updated_at DESC LIMIT ?",
            (self.ext_name, limit)
        )
        logs = []
        for row in cursor.fetchall():
            logs.append({
                "id": row['id'],
                "action": row['callback_name'],
                "status": row['status'],
                "message": row['status_message'],
                "artifact": json.loads(row['artifact_json']) if row['artifact_json'] else {},
                "updated_at": row['updated_at']
            })
        return logs

    def cancel(self, job_id):
        """SDK Helper: Cancels a scheduled background job in the Metronome ledger."""
        from akasa.workers import cancel_job as _cancel_job
        return _cancel_job(job_id, workspace_id=self.workspace_id)

    def pause(self, job_id):
        """SDK Helper: Freezes a scheduled background job in the Metronome ledger."""
        from akasa.workers import pause_job as _pause_job
        return _pause_job(job_id, workspace_id=self.workspace_id)

    def resume(self, job_id, reset_timer=True):
        """SDK Helper: Restores a paused background job in the Metronome ledger."""
        from akasa.workers import resume_job as _resume_job
        return _resume_job(job_id, workspace_id=self.workspace_id, reset_timer=reset_timer)
    def update_progress(self, message, artifact=None):
        if self.current_job_id:
            from akasa.workers import update_immediate_job_status as _update_immediate_job_status
            _update_immediate_job_status(self.current_job_id, 'processing', message, artifact, workspace_id=self.workspace_id)

    def update_meta(self, meta_dict):
        if self.current_job_id:
            from akasa.workers import update_immediate_job_meta as _update_immediate_job_meta
            _update_immediate_job_meta(self.current_job_id, meta_dict, workspace_id=self.workspace_id)
_REGISTERED_SETTINGS_SCHEMAS = {}
_WORKSPACES_MIGRATED = False
MASKED_SECRET_SENTINEL = "••••••••"


def get_registered_settings_schemas(workspace_id: Optional[str] = None) -> Dict[str, List[Dict[str, Any]]]:
    """Evaluates and returns all registered declarative settings schemas for the target workspace."""
    wid = workspace_id or "default"
    evaluated: Dict[str, List[Dict[str, Any]]] = {}
    for ext_id, schema_spec in _REGISTERED_SETTINGS_SCHEMAS.items():
        if callable(schema_spec):
            try:
                evaluated[ext_id] = schema_spec(wid)
            except Exception:
                evaluated[ext_id] = []
        else:
            evaluated[ext_id] = schema_spec
    return evaluated

class SettingsManager:
    def __init__(self, ext_name, workspace_id, schema=None):
        self.ext_name = ext_name
        self.workspace_id = workspace_id
        self.secrets_filename = "secrets.json"
        self.workspace_filename = "workspace.settings.json"
        self.repo_filename = "repo.settings.json"
        if schema is None:
            schema = _REGISTERED_SETTINGS_SCHEMAS.get(ext_name)
        self.schema = schema(workspace_id) if callable(schema) else (schema or [])

    def _get_field_meta(self, key):
        for field in self.schema:
            if field.get('id') == key:
                return field
        return None
    def _resolve_filepath(self, scope):
        control_dir_name = os.environ.get("AKASA_CONTROL_DIR", ".akasa")
        if scope == 'daemon':
            return Path(_akasa_utils._cwd).joinpath(control_dir_name, "host.json").as_posix()

        control_dir = get_tenant_control_dir(self.workspace_id)
        if scope == 'repo':
            return Path(control_dir).joinpath(self.repo_filename).as_posix()
        else:
            return Path(control_dir).joinpath(self.workspace_filename).as_posix()
    def get(self, key, default=None, repo=None, mask_secrets=False):
        field = self._get_field_meta(key)
        if not field:
            return default

        scope = field.get('scope', 'workspace')
        is_secure = field.get('secure', False)

        if is_secure:
            control_dir = get_tenant_control_dir(self.workspace_id)
            filepath = Path(control_dir).joinpath(self.secrets_filename).as_posix()
            data = load_json_config(filepath, {})

            ext_secrets = data.get(self.ext_name, {})
            if scope == 'repo' and repo:
                ext_secrets = ext_secrets.get(repo, {})

            if key in ext_secrets:
                val = ext_secrets[key]
                decrypted = val
                if isinstance(val, str) and val.startswith("v1:"):
                    from akasa.auth import decrypt_secret as _decrypt_secret
                    decrypted = _decrypt_secret(val)
                elif val:
                    from akasa.auth import encrypt_secret as _encrypt_secret
                    enc_val = _encrypt_secret(str(val))
                    if scope == 'repo' and repo:
                        data.setdefault(self.ext_name, {}).setdefault(repo, {})[key] = enc_val
                    else:
                        data.setdefault(self.ext_name, {})[key] = enc_val
                    save_json_config(filepath, data, self.workspace_id)
                    decrypted = val
                if mask_secrets:
                    return MASKED_SECRET_SENTINEL if decrypted else ""
                return decrypted
        else:
            filepath = self._resolve_filepath(scope)
            data = load_json_config(filepath, {})

            if scope == 'daemon':
                # Agnostic Fallback: If it is nested under the extension, return it.
                # Otherwise, if it manages a root configuration parameter, return the root.
                if key in data.get(self.ext_name, {}):
                    return data[self.ext_name][key]
                elif key in data:
                    return data[key]
            elif scope == 'repo':
                if repo and key in data.get(repo, {}).get(self.ext_name, {}):
                    return data[repo][self.ext_name][key]
            else:
                if key in data.get(self.ext_name, {}):
                    return data[self.ext_name][key]

        return field.get('default', default)
    def set(self, key, value, repo=None):
        field = self._get_field_meta(key)
        if not field: return

        scope = field.get('scope', 'workspace')
        is_secure = field.get('secure', False)

        if is_secure:
            if value == MASKED_SECRET_SENTINEL:
                return
            if value:
                from akasa.auth import encrypt_secret as _encrypt_secret
                value = _encrypt_secret(value)
            control_dir = get_tenant_control_dir(self.workspace_id)
            filepath = Path(control_dir).joinpath(self.secrets_filename).as_posix()
            data = load_json_config(filepath, {})
            if self.ext_name not in data:
                data[self.ext_name] = {}

            if scope == 'repo' and repo:
                if repo not in data[self.ext_name]:
                    data[self.ext_name][repo] = {}
                data[self.ext_name][repo][key] = value
            else:
                data[self.ext_name][key] = value

            save_json_config(filepath, data, self.workspace_id)
        else:
            filepath = self._resolve_filepath(scope)
            data = load_json_config(filepath, {})

            if scope == 'daemon':
                # Agnostic Write: If the setting already exists at the root level, assume it's a global daemon 
                # parameter and update it there. Otherwise, safely namespace it under the extension's dictionary.
                if key in data and not isinstance(data[key], dict):
                    data[key] = value
                else:
                    if self.ext_name not in data: data[self.ext_name] = {}
                    data[self.ext_name][key] = value
            elif scope == 'repo':
                if repo:
                    if repo not in data: data[repo] = {}
                    if self.ext_name not in data[repo]: data[repo][self.ext_name] = {}
                    data[repo][self.ext_name][key] = value
            else:
                if self.ext_name not in data: data[self.ext_name] = {}
                data[self.ext_name][key] = value
            save_json_config(filepath, data, self.workspace_id)
        hooks.emit('config_mutated', workspace_id=self.workspace_id)
        self._sync_vfs_barrier()
    def get_all(self, repo=None, mask_secrets=False):
        result = {}
        for field in self.schema:
            fid = field.get('id')
            if not fid: continue
            result[fid] = self.get(fid, repo=repo, mask_secrets=mask_secrets)
        return result
    def update(self, payload_dict, repo=None):
        control_dir = get_tenant_control_dir(self.workspace_id)
        daemon_path = self._resolve_filepath('daemon')
        ws_path = self._resolve_filepath('workspace')
        repo_path = self._resolve_filepath('repo')
        secrets_path = Path(control_dir).joinpath(self.secrets_filename).as_posix()

        # Load memory buffers
        buffers = {
            'daemon': load_json_config(daemon_path, {}),
            'workspace': load_json_config(ws_path, {}),
            'repo': load_json_config(repo_path, {}),
            'secure': load_json_config(secrets_path, {})
        }
        dirty = set()

        for field in self.schema:
            k = field.get('id')
            if not k or k not in payload_dict: continue

            val = payload_dict[k]
            scope = field.get('scope', 'workspace')
            is_secure = field.get('secure', False)

            if is_secure:
                if val == MASKED_SECRET_SENTINEL:
                    continue
                if val:
                    from akasa.auth import encrypt_secret as _encrypt_secret
                    val = _encrypt_secret(val)
                if self.ext_name not in buffers['secure']: buffers['secure'][self.ext_name] = {}
                if scope == 'repo' and repo:
                    if repo not in buffers['secure'][self.ext_name]: buffers['secure'][self.ext_name][repo] = {}
                    buffers['secure'][self.ext_name][repo][k] = val
                else:
                    buffers['secure'][self.ext_name][k] = val
                dirty.add('secure')
            else:
                target = buffers[scope]
                if scope == 'daemon':
                    if k in target and not isinstance(target[k], dict):
                        target[k] = val
                    else:
                        if self.ext_name not in target: target[self.ext_name] = {}
                        target[self.ext_name][k] = val
                elif scope == 'repo':
                    if repo:
                        if repo not in target: target[repo] = {}
                        if self.ext_name not in target[repo]: target[repo][self.ext_name] = {}
                        target[repo][self.ext_name][k] = val
                else:
                    if self.ext_name not in target: target[self.ext_name] = {}
                    target[self.ext_name][k] = val
                dirty.add(scope)

        # Commit dirty buffers exactly once
        if 'daemon' in dirty: save_json_config(daemon_path, buffers['daemon'], self.workspace_id)
        if 'workspace' in dirty: save_json_config(ws_path, buffers['workspace'], self.workspace_id)
        if 'repo' in dirty: save_json_config(repo_path, buffers['repo'], self.workspace_id)
        if 'secure' in dirty: save_json_config(secrets_path, buffers['secure'], self.workspace_id)
        if dirty:
            self._sync_vfs_barrier()
            hooks.emit('config_mutated', workspace_id=self.workspace_id)
    def _sync_vfs_barrier(self, timeout=5.0):
        """Internal barrier to guarantee read-after-write consistency for settings."""
        _vfs_sync_barrier(self.workspace_id, timeout=timeout)

class StoreManager:
    def __init__(self, workspace_id, ext_name="system"):
        self.workspace_id = workspace_id
        self.ext_name = ext_name
    def _resolve_store_path(self, filename):
        control_dir = get_tenant_control_dir(self.workspace_id)
        data_dir = Path(control_dir).joinpath("data", self.ext_name)
        target_path = data_dir.joinpath(filename)
        if not target_path.exists():
            legacy_candidates = [
                Path(control_dir).joinpath("data", filename),
                Path(control_dir).joinpath(filename),
                Path(control_dir).joinpath("ext", self.ext_name, "data", filename),
                Path(control_dir).joinpath("ext", self.ext_name, filename),
            ]
            for cand in legacy_candidates:
                if cand.exists() and cand.is_file():
                    try:
                        os.makedirs(data_dir.as_posix(), exist_ok=True)
                        shutil.move(cand.as_posix(), target_path.as_posix())
                        break
                    except Exception:
                        return cand.as_posix()
        os.makedirs(data_dir.as_posix(), exist_ok=True)
        return target_path.as_posix()

    def get(self, filename, key, default=None):
        filepath = self._resolve_store_path(filename)
        data = load_json_config(filepath, {})
        return data.get(key, default)

    def set(self, filename, key, value):
        filepath = self._resolve_store_path(filename)
        data = load_json_config(filepath, {})
        data[key] = value
        save_json_config(filepath, data, self.workspace_id)
class DatabaseWrapper:
    """Lightweight ORM/CRUD wrapper for centralized SQLite access."""
    def __init__(self, conn):
        self._conn = conn

    def __enter__(self):
        self._conn.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self._conn.__exit__(exc_type, exc_val, exc_tb)

    def __getattr__(self, name):
        return getattr(self._conn, name)

    def get_all(self, table, order_by=None):
        query = f"SELECT * FROM {table}"
        if order_by:
            query += f" ORDER BY {order_by}"
        cursor = self._conn.execute(query)
        return [dict(row) for row in cursor.fetchall()]
    def insert_or_replace(self, table, data):
        keys = list(data.keys())
        values = tuple(data[k] for k in keys)
        placeholders = ", ".join(["?"] * len(keys))
        cols = ", ".join(keys)
        self._conn.execute(f"INSERT OR REPLACE INTO {table} ({cols}) VALUES ({placeholders})", values)
        self._conn.commit()

    def update(self, table, data, where_col, where_val):
        keys = list(data.keys())
        values = tuple(data[k] for k in keys) + (where_val,)
        set_clause = ", ".join([f"{k} = ?" for k in keys])
        self._conn.execute(f"UPDATE {table} SET {set_clause} WHERE {where_col} = ?", values)
        self._conn.commit()

    def get_by_id(self, table, id_val, id_col="id"):
        cursor = self._conn.execute(f"SELECT * FROM {table} WHERE {id_col} = ?", (id_val,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def delete(self, table, where_col, where_val):
        self._conn.execute(f"DELETE FROM {table} WHERE {where_col} = ?", (where_val,))
        self._conn.commit()
class ExtensionContext:
    """Pre-scoped context object injected into all SDK routes."""
    def __init__(self, ext_name, workspace_id, settings_schema=None, job_id=None):
        self.ext_name = ext_name
        self.workspace_id = workspace_id
        self.vfs = VFSTransaction(workspace_id)
        self.req = request
        self.jobs = JobManager(ext_name, workspace_id, job_id=job_id)
        self.store = StoreManager(workspace_id, ext_name=ext_name)
        self.settings = SettingsManager(ext_name, workspace_id, schema=settings_schema)
    def get_db(self, db_name: Optional[str] = None) -> DatabaseWrapper:
        """Returns a DatabaseWrapper for the specified SQLite database name (defaults to self.ext_name) in the active workspace."""
        conn = get_connection(db_name or self.ext_name, self.workspace_id)
        return DatabaseWrapper(conn)

    @property
    def db(self) -> DatabaseWrapper:
        """Returns an SQLite connection automatically keyed to the active tenant workspace."""
        return self.get_db(self.ext_name)

    def get_active_modules(self) -> List[str]:
        """Returns a sorted list of all active core and workspace-enabled extension modules."""
        return get_active_modules(self.workspace_id)

    def emit_sse(self, event_type: str, payload: Dict[str, Any]) -> None:
        """Publishes a real-time Server-Sent Event onto the global SSE bus."""
        sse_bus.emit_sse(event_type, payload)

    def get_repo_data_path(self, repo: str, subpath: str = "") -> str:
        """Returns the canonical VFS URI for sovereign extension data inside a target repository."""
        ctrl_name = os.environ.get("AKASA_CONTROL_DIR", ".akasa")
        clean_sub = str(subpath).lstrip('/')
        base = f"vfs://{repo}/{ctrl_name}-data/{self.ext_name}"
        return f"{base}/{clean_sub}" if clean_sub else base

    def get_shared_mounts(self, domain: Optional[str] = None) -> List[Dict[str, str]]:
        """Returns active peer workspace mounts matching this extension's domain: [{'workspace_id': ..., 'domain': ..., 'uri': 'ws://...'}]."""
        target_domain = domain or self.ext_name
        sys_settings = SettingsManager('system', self.workspace_id)
        shared_mounts = sys_settings.get("shared_mounts") or []
        results = []
        seen = set()
        for mount in shared_mounts:
            if not isinstance(mount, dict):
                continue
            if mount.get("domain") in (target_domain, "*"):
                target_ws = mount.get("workspace_id")
                if not target_ws or target_ws == self.workspace_id or target_ws in seen:
                    continue
                seen.add(target_ws)
                results.append({
                    "workspace_id": target_ws,
                    "domain": target_domain,
                    "uri": f"ws://{target_ws}/{target_domain}"
                })
        return results

    def iter_shared_mount_files(self, domain: Optional[str] = None, exts: Optional[List[str]] = None):
        """Yields (source_workspace_id, ws_file_uri) across all peer mounts subscribed to this domain."""
        for m in self.get_shared_mounts(domain=domain):
            for ws_file_uri in self.vfs.walk(m["uri"], exts=exts):
                yield m["workspace_id"], ws_file_uri

    @property
    def paths(self):
        """Returns the active tenant's structural path dictionary."""
        cfg_path, ws_root = get_workspace_physics(self.workspace_id)
        workspace_dir = Path(cfg_path).parent.as_posix()
        data_dir = Path(workspace_dir).joinpath("data", self.ext_name).as_posix()
        cache_dir = Path(workspace_dir).joinpath("cache", self.ext_name).as_posix()
        os.makedirs(data_dir, exist_ok=True)
        os.makedirs(cache_dir, exist_ok=True)
        base_paths = {
            "config_path": cfg_path,
            "control_dir": workspace_dir,
            "workspace_root": ws_root,
            "data_dir": data_dir,
            "cache_dir": cache_dir,
            "artifacts_base": cache_dir
        }

        # Agnostically merge domain-specific paths injected by Tier 2 engines
        base_paths.update(hooks.emit_merge_dict('request_paths', workspace_id=self.workspace_id))
        return base_paths
    @property
    def config(self):
        """Lazily fetches the active tenant's workspace configuration."""
        return load_config(self.workspace_id)
    @property
    def exec(self):
        """Returns a sandboxed ProcessManager keyed to the active tenant workspace."""
        from akasa.process import ProcessManager as _ProcessManager
        return _ProcessManager(self.workspace_id)
    def resolve_path(self, filepath):
        """Safely anchors a relative path to the physical workspace bounds."""
        return _resolve_physical_path(filepath, self.workspace_id)

    def find_path_candidates(self, query_path, **kwargs):
        """Opt-in candidate discovery for path disambiguation."""
        return self.emit_first('find_path_candidates', query_path=query_path, default=[], **kwargs)

    def resolve_dag_chain(self, hook_name: str):
        """SDK Helper: Resolves an ordered compilation/execution DAG chain for the current workspace."""
        from akasa.workers import resolve_dag_chain as _resolve_dag_chain
        return _resolve_dag_chain(self, hook_name)

    def parse_uri(self, path_str):
        return parse_uri(path_str)

    def expand_selection(self, items, gather_only=False):
        """
        SSOT: Expands polymorphic frontend selection items into a flat, 
        deduplicated, and stable-sorted list of physical or logical filepaths.
        Enforces a strict List[str] return contract with native VFS directory expansion fallback.
        """
        from akasa.uri import AkasaURI as _AkasaURI

        candidate_list = self.emit_first(
            'expand_selection',
            items=items,
            gather_only=gather_only,
            default=None,
            predicate=lambda r: r is not None
        )
        source_iterable = candidate_list if candidate_list is not None else (items or [])

        normalized_files: List[str] = []
        for entry in source_iterable:
            if isinstance(entry, str):
                raw_str = entry.strip()
                if not raw_str:
                    continue
                uri_obj = _AkasaURI(raw_str)
                if uri_obj.is_dir:
                    for walked in self.vfs.walk(str(uri_obj).rstrip('/')):
                        if not str(walked).endswith(('.gitkeep', '.keep')):
                            normalized_files.append(str(walked))
                else:
                    normalized_files.append(str(uri_obj))
            elif isinstance(entry, dict):
                raw_target = str(entry.get("filepath") or entry.get("folderpath") or entry.get("id") or "").strip()
                if not raw_target:
                    continue
                is_dir = bool(entry.get("isDir") or entry.get("folderpath") or raw_target.endswith('/'))
                if is_dir:
                    for walked in self.vfs.walk(raw_target.rstrip('/')):
                        if not str(walked).endswith(('.gitkeep', '.keep')):
                            normalized_files.append(str(walked))
                else:
                    normalized_files.append(str(_AkasaURI(raw_target)))
        return list(dict.fromkeys(normalized_files))

    def sync_vfs_barrier(self, timeout=5.0):
        """Halts the current thread until all VFS writes queued prior to this call are physically flushed."""
        _vfs_sync_barrier(self.workspace_id, timeout=timeout)
    def emit(self, event_name, *args, **kwargs):
        """Emits a synchronous event, automatically injecting the tenant's workspace ID if not explicitly specified."""
        kwargs.setdefault('workspace_id', self.workspace_id)
        return hooks.emit(event_name, *args, **kwargs)

    def emit_first(self, event_name, *args, default=None, predicate=None, **kwargs):
        """Emits a short-circuiting query event, automatically injecting the tenant's workspace ID if not explicitly specified."""
        kwargs.setdefault('workspace_id', self.workspace_id)
        return hooks.emit_first(event_name, *args, default=default, predicate=predicate, **kwargs)

    def emit_merge_dict(self, event_name, *args, **kwargs) -> Dict[str, Any]:
        """Emits a dictionary-merging query event, automatically injecting the tenant's workspace ID if not explicitly specified."""
        kwargs.setdefault('workspace_id', self.workspace_id)
        return hooks.emit_merge_dict(event_name, *args, **kwargs)

    def emit_merge_list(self, event_name, *args, **kwargs) -> List[Any]:
        """Emits a list-flattening query event, automatically injecting the tenant's workspace ID if not explicitly specified."""
        kwargs.setdefault('workspace_id', self.workspace_id)
        return hooks.emit_merge_list(event_name, *args, **kwargs)

    def emit_background(self, event_name, *args, **kwargs):
        """Emits a background event, automatically injecting the tenant's workspace ID if not explicitly specified."""
        kwargs.setdefault('workspace_id', self.workspace_id)
        return hooks.emit_background(event_name, *args, **kwargs)


class AkasaExtension:
    """
    Agnostic Blueprint wrapper that enforces tenant isolation, API routing,
    authorization, and SQLite schema migrations for kernel extensions.
    """
    context_class = ExtensionContext

    def __init__(self, name, module_name, title=None, description="", schema=None, core=False, settings_schema=None, extra_schemas=None):
        self.name = name
        self.title = title or name.replace('_', ' ').title()
        self.description = description
        self.bp = Blueprint(name, module_name)
        self.schema = schema or {}
        self.core = core
        self.settings_schema = settings_schema or []
        self.extra_schemas = extra_schemas or {}

        if self.settings_schema:
            _REGISTERED_SETTINGS_SCHEMAS[self.name] = self.settings_schema
        if self.schema:
            register_schema(self.name, self.schema)
        for db_name, db_schema in self.extra_schemas.items():
            register_schema(db_name, db_schema)
        # Universal REST endpoints for extension settings management
        @self.route('settings', methods=['GET'], docstring="[sync] Retrieves the current settings for this extension.")
        def get_settings(ctx):
            repo = ctx.req.args.get('repo')
            return jsonify(ctx.settings.get_all(repo=repo, mask_secrets=True))
        @self.route('settings', methods=['POST'], response_schema=SettingsUpdateResponse, docstring="[sync] Updates the settings for this extension.")
        def update_settings(ctx):
            try:
                repo = ctx.req.args.get('repo') or (ctx.req.json.get('_repo') if ctx.req.json else None)
                ctx.settings.update(ctx.req.json or {}, repo=repo)
            except Exception as e:
                print(f"Settings Save Error: {traceback.format_exc()}")
                return jsonify({"status": "error", "error": f"Internal save error: {str(e)}"}), 500
            # Emit a strictly scoped event for this specific extension
            results = ctx.emit(f'{self.name}_settings_updated')

            job_id = None
            requires_refresh = False
            for res in results:
                if isinstance(res, dict):
                    if "job_id" in res:
                        job_id = res["job_id"]
                    if res.get("requires_refresh"):
                        requires_refresh = True
            payload = {"status": "success", "requires_refresh": requires_refresh}
            if job_id:
                payload["job_id"] = job_id
            return jsonify(payload)
        @self.route('settings/schema', methods=['GET'], docstring="[sync] Retrieves the declarative settings schema for this extension.")
        def get_settings_schema(ctx):
            schema = self.settings_schema(ctx.workspace_id) if callable(self.settings_schema) else self.settings_schema
            return jsonify(schema)

    def register_schema(self, db_name: str, schema_dict: Dict[str, Dict[str, str]]) -> None:
        """Registers an additional declarative SQLite schema owned by this extension."""
        register_schema(db_name, schema_dict)

    def get_context(self, workspace_id=None, job_id=None):
        """SDK Factory: Returns a fully hydrated context inherently bound to this extension's schema."""
        wid = workspace_id or _akasa_utils.sniff_tenant_id() or "default"
        return self.context_class(self.name, wid, settings_schema=self.settings_schema, job_id=job_id)

    def on(self, event_name: str, priority: int = 50):
        """
        SDK Hook Decorator: Registers an event listener stamped with this extension's authorization identity.
        Automatically hydrates and injects `ctx` if the function's first parameter is named `ctx`.
        """
        def decorator(func):
            sig = inspect.signature(func)
            params = list(sig.parameters.keys())
            wants_ctx = bool(params and params[0] == 'ctx')

            if wants_ctx:
                @functools.wraps(func)
                def wrapper(*args, **kwargs):
                    if args and isinstance(args[0], ExtensionContext):
                        return func(*args, **kwargs)
                    wid = kwargs.get('workspace_id') or _akasa_utils.sniff_tenant_id() or "default"
                    ctx = self.get_context(wid)
                    return func(ctx, *args, **kwargs)

                wrapper._akasa_ext_name = self.name
                wrapper.is_core = self.core
                hooks.on(event_name, priority=priority)(wrapper)
                return wrapper
            else:
                func._akasa_ext_name = self.name
                func.is_core = self.core
                hooks.on(event_name, priority=priority)(func)
                return func
        return decorator

    def worker(self, task_name):
        """
        SDK Decorator: Automatically wraps a background worker in a safe execution block.
        Injects the ExtensionContext, catches exceptions, and updates the immediate_jobs ledger.
        """
        def decorator(f):
            from akasa.workers import register_callback as _register_callback

            # Do not use functools.wraps here. The metronome relies on inspect.signature
            # to dynamically inject job_id and workspace_id based on the wrapper's exact kwargs.
            def wrapper(job_id=None, workspace_id=None, **kwargs):
                ctx = self.get_context(workspace_id, job_id=job_id)

                # Execute the domain logic
                if 'job_id' in inspect.signature(f).parameters:
                    kwargs['job_id'] = job_id
                return f(ctx, **kwargs)

            _register_callback(self.name, task_name, wrapper)
            return wrapper
        return decorator
    def route(self, rule, request_schema=None, response_schema=None, **options):
        # SDK Guardrail: Ban empty routes to prevent 308 Mixed Content redirect traps behind reverse proxies
        if rule in ['', '/']:
            raise ValueError(f"SDK Exception in '{self.name}': Empty root routes ('' or '/') are banned. Please use an explicit endpoint name like 'list' or 'index'.")

        # Auto-Routing: Enforce strict stateless multi-tenant boundaries natively
        clean_rule = rule.lstrip('/')
        full_rule = f'/api/<workspace_id>/{self.name}/{clean_rule}'
        # OS Reflection: Register structural contracts for automated OpenAPI generation
        # Attach directly to the underlying Flask Blueprint instance for Flask reflection
        if not hasattr(self.bp, 'route_schemas'):
            self.bp.route_schemas = {}
            
        if full_rule not in self.bp.route_schemas:
            self.bp.route_schemas[full_rule] = {}
            
        docstring = options.pop('docstring', None)
        for method in options.get('methods', ['GET']):
            m_upper = method.upper()
            req_sch = request_schema.get(m_upper) if isinstance(request_schema, dict) else request_schema
            res_sch = response_schema.get(m_upper) if isinstance(response_schema, dict) else response_schema
            
            self.bp.route_schemas[full_rule][m_upper] = {
                "request_schema": req_sch,
                "response_schema": res_sch,
                "docstring": docstring
            }

        def decorator(f):
            @functools.wraps(f)
            def wrapper(workspace_id, *args, **kwargs):
                ctx = self.get_context(workspace_id)
                return f(ctx, *args, **kwargs)

            # Use getattr to safely bypass the auth gate for core OS engines
            if not getattr(self, 'core', False):
                wrapper = extension_auth(self.name)(wrapper)
            self.bp.route(full_rule, **options)(wrapper)
            return wrapper
        return decorator