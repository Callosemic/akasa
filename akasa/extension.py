import os
import re
import uuid
import json
import time
import shutil
import inspect
import traceback
import threading
import functools
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Dict, Any, List, TypedDict, Callable
from flask import Blueprint, Response, request, jsonify, has_request_context
if TYPE_CHECKING:
    from akasa.types import WorkspaceID, FilePath, VFSStorageEngine, WorkspacePathsDict
from akasa.uri import AkasaURI
from akasa.context import (
    execution_frame,
    get_current_frame,
    sniff_tenant_id,
    get_control_dir_name,
    parse_uri,
    atomic_write_text,
)
from akasa.events import sse_bus
from akasa.hooks import hooks
from akasa.db import get_connection, register_schema
from akasa.vfs import VFSTransaction, _resolve_physical_path, sync_vfs_barrier as _vfs_sync_barrier
from akasa.state import (
    load_config,
    get_workspace_physics,
    get_tenant_control_dir,
    get_daemon_host_config_path,
    load_json_config,
    save_json_config,
    get_active_modules,
    is_extension_enabled,
)
from akasa.process import ProcessManager
from akasa.workers import (
    submit_immediate_job,
    submit_one_shot_job,
    submit_job,
    validate_worker_kwargs,
    register_ephemeral_artifact,
    start_dag_chain,
    run_parallel,
    wait_for_jobs,
    cancel_job,
    pause_job,
    resume_job,
    update_immediate_job_status,
    update_immediate_job_meta,
    resolve_dag_chain,
    register_callback,
)
import akasa.auth as _akasa_auth
import akasa.utils as _akasa_utils
from akasa.auth import encrypt_secret, decrypt_secret, get_master_key


def extension_auth(ext_name: str):
    """HTTP route decorator verifying that ext_name is enabled in the target workspace."""
    def decorator(f):
        @functools.wraps(f)
        def decorated_function(*args, **kwargs):
            ws_id = kwargs.get('workspace_id') or sniff_tenant_id()
            if not is_extension_enabled(ext_name, ws_id):
                return jsonify({"error": f"403 Forbidden: Extension '{ext_name}' is not enabled in this workspace."}), 403
            return f(*args, **kwargs)
        return decorated_function
    return decorator

class SettingsUpdateResponse(TypedDict, total=False):
    status: str
    requires_refresh: bool
    job_id: str
    error: str
class JobManager:
    def __init__(self, ext_name, workspace_id, job_id=None, root_job_id=None, chain_state=None, chain_history=None):
        self.ext_name = ext_name
        self.workspace_id = workspace_id
        self.current_job_id = job_id
        self.root_job_id = root_job_id or job_id
        self.chain_state = dict(chain_state) if isinstance(chain_state, dict) else {}
        self.chain_history = list(chain_history) if isinstance(chain_history, list) else []
    def submit(self, task_name, coalesce=False, job_category="system_background", job_id=None, **kwargs):
        validate_worker_kwargs(self.ext_name, task_name, kwargs, allow_extra=False)
        job_prefix = self.ext_name[:3].lower()
        resolved_id = job_id or f"{job_prefix}_{uuid.uuid4().hex[:8]}"
        args_json = json.dumps(kwargs)
        return submit_immediate_job(
            resolved_id, self.ext_name, task_name, args_json, self.workspace_id, coalesce=coalesce, job_category=job_category
        )

    def submit_one_shot(self, task_name, delay_ms, job_id=None, **kwargs):
        kwargs.pop("job_category", None)
        validate_worker_kwargs(self.ext_name, task_name, kwargs, allow_extra=False)
        job_prefix = self.ext_name[:3].lower()
        resolved_id = job_id or f"{job_prefix}_{uuid.uuid4().hex[:8]}"
        args_json = json.dumps(kwargs)
        submit_one_shot_job(resolved_id, self.ext_name, task_name, delay_ms, args_json, self.workspace_id)
        return resolved_id

    def schedule(self, task_name, interval_ms, jitter_ms=0, job_id=None, **kwargs):
        """SDK Helper: Schedules a recurring task in the Metronome ledger."""
        kwargs.pop("job_category", None)
        validate_worker_kwargs(self.ext_name, task_name, kwargs, allow_extra=False)
        job_prefix = self.ext_name[:3].lower()
        resolved_id = job_id or f"{job_prefix}_{uuid.uuid4().hex[:8]}"
        args_json = json.dumps(kwargs)
        submit_job(resolved_id, self.ext_name, task_name, interval_ms, args_json, jitter_ms=jitter_ms, workspace_id=self.workspace_id)
        return resolved_id
    def register_ephemeral(self, filepath, ttl_seconds=3600, owner=None):
        """SDK Helper: Registers a temporary file artifact for automatic TTL garbage collection."""
        register_ephemeral_artifact(filepath, owner or self.ext_name, ttl_seconds, workspace_id=self.workspace_id)

    def write_scratch(self, filename: str, content: str, ttl_seconds: int = 3600, owner: Optional[str] = None) -> str:
        """Atomically writes an ephemeral scratch buffer inside the tenant cache directory and registers it for TTL cleanup."""
        control_dir = get_tenant_control_dir(self.workspace_id)
        scratch_dir = Path(control_dir).joinpath("cache", self.ext_name, "scratch")
        clean_name = Path(filename).name
        scratch_file = scratch_dir.joinpath(clean_name).as_posix()
        atomic_write_text(scratch_file, content)
        register_ephemeral_artifact(scratch_file, owner or f"{self.ext_name}_scratch", ttl_seconds, workspace_id=self.workspace_id)
        return scratch_file

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
        if not steps:
            return None
        return start_dag_chain(
            steps,
            self.workspace_id,
            on_complete_hook=on_complete_hook,
            job_category=job_category,
            **kwargs
        )

    def map_parallel(self, fn, items) -> list:
        """SDK Helper: Executes fn(item) across items using the shared bounded kernel I/O pool."""
        return run_parallel(fn, items)

    def wait_for(self, prefix: str = None, job_id: str = None, timeout: float = 10.0) -> bool:
        """SDK Helper: Blocks until matching in-flight immediate jobs complete via in-memory threading.Event."""
        return wait_for_jobs(self.workspace_id, prefix=prefix, job_id=job_id, timeout=timeout)

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
    def find_active_pipeline_job(
        self,
        worker_names: List[str],
        predicate: Optional[Callable[[Dict[str, Any]], bool]] = None,
        window_sec: float = 300.0,
    ) -> Optional[str]:
        """SDK Helper: Checks if any of the given pipeline worker names are actively running and satisfy the optional predicate."""
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
                job_args = json.loads(existing_job['args_json'] or "{}")
                if predicate is None or predicate(job_args):
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
        return cancel_job(job_id, workspace_id=self.workspace_id)

    def pause(self, job_id):
        """SDK Helper: Freezes a scheduled background job in the Metronome ledger."""
        return pause_job(job_id, workspace_id=self.workspace_id)

    def resume(self, job_id, reset_timer=True):
        """SDK Helper: Restores a paused background job in the Metronome ledger."""
        return resume_job(job_id, workspace_id=self.workspace_id, reset_timer=reset_timer)

    def update_progress(self, message, artifact=None):
        if self.current_job_id:
            update_immediate_job_status(self.current_job_id, 'processing', message, artifact, workspace_id=self.workspace_id)

    def update_meta(self, meta_dict):
        if self.current_job_id:
            update_immediate_job_meta(self.current_job_id, meta_dict, workspace_id=self.workspace_id)
_REGISTERED_SETTINGS_SCHEMAS = {}
_CAPABILITY_REGISTRY: Dict[str, Dict[str, Any]] = {}
_EXTENSION_INSTANCES: Dict[str, "AkasaExtension"] = {}
_WORKSPACES_MIGRATED = False
MASKED_SECRET_SENTINEL = "••••••••"


def get_registered_capabilities() -> Dict[str, Dict[str, Any]]:
    """Returns a shallow copy of the global transport-agnostic capability registry."""
    return dict(_CAPABILITY_REGISTRY)


def _extract_http_request_payload(extra_kwargs: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Merges query parameters, form fields, JSON body, and URL path variables into a single dict."""
    merged: Dict[str, Any] = {}
    if has_request_context():
        try:
            for k, v in request.args.items():
                merged[k] = v
        except Exception:
            pass
        try:
            if request.form:
                for k, v in request.form.items():
                    merged[k] = v
        except Exception:
            pass
        try:
            body = request.get_json(silent=True)
            if isinstance(body, dict):
                merged.update(body)
        except Exception:
            pass
    if extra_kwargs:
        merged.update(extra_kwargs)
    return merged
def _marshal_http_return(result: Any):
    """Marshals pure Python dict/list/tuple/str capability returns into valid Flask HTTP responses."""
    if isinstance(result, Response):
        return result
    if isinstance(result, tuple):
        body = result[0]
        rest = result[1:]
        if isinstance(body, (dict, list)):
            return (jsonify(body), *rest)
        return result
    if isinstance(result, (dict, list)):
        return jsonify(result)
    return result


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
        if scope == 'daemon':
            return get_daemon_host_config_path()

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
                    decrypted = decrypt_secret(val)
                elif val:
                    enc_val = encrypt_secret(str(val))
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
            found, stored = self._lookup_stored_plain(scope, key, repo)
            if found:
                return stored

        return field.get('default', default)

    def _lookup_stored_plain(self, scope, key, repo=None):
        """Returns (found, value) for a non-secure key from its scope's settings file, without schema defaults."""
        data = load_json_config(self._resolve_filepath(scope), {})
        if scope == 'daemon':
            # Agnostic Fallback: If it is nested under the extension, return it.
            # Otherwise, if it manages a root configuration parameter, return the root.
            if key in data.get(self.ext_name, {}):
                return True, data[self.ext_name][key]
            if key in data:
                return True, data[key]
        elif scope == 'repo':
            if repo and key in data.get(repo, {}).get(self.ext_name, {}):
                return True, data[repo][self.ext_name][key]
        else:
            if key in data.get(self.ext_name, {}):
                return True, data[self.ext_name][key]
        return False, None

    def is_set(self, key, repo=None) -> bool:
        """
        True when a value for key has been saved in its scope's storage, False when get() would
        fall back to the schema default. Lets callers distinguish explicit configuration from defaults.
        """
        field = self._get_field_meta(key)
        if not field:
            return False
        scope = field.get('scope', 'workspace')
        if field.get('secure', False):
            control_dir = get_tenant_control_dir(self.workspace_id)
            data = load_json_config(Path(control_dir).joinpath(self.secrets_filename).as_posix(), {})
            ext_secrets = data.get(self.ext_name, {})
            if scope == 'repo' and repo:
                ext_secrets = ext_secrets.get(repo, {})
            return key in ext_secrets
        found, _ = self._lookup_stored_plain(scope, key, repo)
        return found
    def set(self, key, value, repo=None):
        field = self._get_field_meta(key)
        if not field: return

        scope = field.get('scope', 'workspace')
        is_secure = field.get('secure', False)
        if is_secure:
            if value == MASKED_SECRET_SENTINEL:
                return
            if value:
                value = encrypt_secret(value)
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
        scope_paths = {
            'daemon': self._resolve_filepath('daemon'),
            'workspace': self._resolve_filepath('workspace'),
            'repo': self._resolve_filepath('repo'),
            'secure': Path(control_dir).joinpath(self.secrets_filename).as_posix(),
        }
        buffers: Dict[str, dict] = {}
        dirty = set()

        def _get_buffer(target_scope: str) -> dict:
            if target_scope not in buffers:
                buffers[target_scope] = load_json_config(scope_paths[target_scope], {})
            return buffers[target_scope]

        for field in self.schema:
            k = field.get('id')
            if not k or k not in payload_dict:
                continue

            val = payload_dict[k]
            scope = field.get('scope', 'workspace')
            is_secure = field.get('secure', False)

            if is_secure:
                if val == MASKED_SECRET_SENTINEL:
                    continue
                if val:
                    val = encrypt_secret(val)
                sec_buf = _get_buffer('secure')
                if self.ext_name not in sec_buf:
                    sec_buf[self.ext_name] = {}
                if scope == 'repo' and repo:
                    if repo not in sec_buf[self.ext_name]:
                        sec_buf[self.ext_name][repo] = {}
                    sec_buf[self.ext_name][repo][k] = val
                else:
                    sec_buf[self.ext_name][k] = val
                dirty.add('secure')
            else:
                target = _get_buffer(scope)
                if scope == 'daemon':
                    if k in target and not isinstance(target[k], dict):
                        target[k] = val
                    else:
                        if self.ext_name not in target:
                            target[self.ext_name] = {}
                        target[self.ext_name][k] = val
                elif scope == 'repo':
                    if repo:
                        if repo not in target:
                            target[repo] = {}
                        if self.ext_name not in target[repo]:
                            target[repo][self.ext_name] = {}
                        target[repo][self.ext_name][k] = val
                else:
                    if self.ext_name not in target:
                        target[self.ext_name] = {}
                    target[self.ext_name][k] = val
                dirty.add(scope)

        # Commit dirty buffers exactly once
        for dirty_scope in dirty:
            save_json_config(scope_paths[dirty_scope], buffers[dirty_scope], self.workspace_id)
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
    def __init__(self, ext_name, workspace_id, settings_schema=None, job_id=None, root_job_id=None, chain_state=None, chain_history=None, payload=None):
        self.ext_name = ext_name
        self.workspace_id = workspace_id
        self.vfs = VFSTransaction(workspace_id)
        self.req = request
        self._payload = dict(payload) if isinstance(payload, dict) else None
        self.jobs = JobManager(
            ext_name,
            workspace_id,
            job_id=job_id,
            root_job_id=root_job_id,
            chain_state=chain_state,
            chain_history=chain_history,
        )
        self.store = StoreManager(workspace_id, ext_name=ext_name)
        self.settings = SettingsManager(ext_name, workspace_id, schema=settings_schema)
    @property
    def method(self) -> str:
        """Returns the active HTTP method ('GET', 'POST', etc.) or 'CALL' when invoked via ctx.call()."""
        frame = get_current_frame()
        if frame is not None and frame.method:
            return frame.method
        if has_request_context():
            try:
                return str(request.method or "GET").upper()
            except Exception:
                return "GET"
        return "CALL"

    @property
    def payload(self) -> Dict[str, Any]:
        """Returns the unified input dictionary across HTTP query/JSON/path params and in-process ctx.call()."""
        if self._payload is not None:
            return self._payload
        self._payload = _extract_http_request_payload()
        return self._payload

    def call(self, capability: str, wait: bool = True, timeout: float = 60.0, **kwargs) -> Any:
        """
        In-Process Zero-Copy IPC Gateway:
        Invokes a registered capability ('<ext>.<action>') within the active workspace tenant boundary,
        automatically awaiting 202 background jobs when wait=True.
        """
        cap_key = str(capability or "").strip()
        spec = _CAPABILITY_REGISTRY.get(cap_key)
        if not spec:
            alt_key = cap_key.replace("/", ".")
            spec = _CAPABILITY_REGISTRY.get(alt_key)
        if not spec:
            raise KeyError(f"Capability '{capability}' is not registered in the Akasa capability registry.")

        target_ext = spec["ext_name"]
        if not spec.get("core", False) and not is_extension_enabled(target_ext, self.workspace_id):
            raise PermissionError(f"403 Forbidden: Extension '{target_ext}' is not enabled in workspace '{self.workspace_id}'.")

        ext_obj = _EXTENSION_INSTANCES.get(target_ext)
        call_payload = dict(kwargs)
        target_ctx = (
            ext_obj.get_context(self.workspace_id, payload=call_payload)
            if ext_obj
            else ExtensionContext(target_ext, self.workspace_id, payload=call_payload)
        )
        method_override = str(call_payload.pop("_method", "") or "").upper()
        handlers_by_method = spec.get("handlers_by_method") or {}
        if method_override and method_override in handlers_by_method:
            handler = handlers_by_method[method_override]
        elif len(handlers_by_method) > 1:
            if call_payload and "POST" in handlers_by_method:
                handler = handlers_by_method["POST"]
            elif not call_payload and "GET" in handlers_by_method:
                handler = handlers_by_method["GET"]
            else:
                handler = spec["handler"]
        else:
            handler = spec["handler"]
        sig = inspect.signature(handler)
        accepted_kwargs = {}
        accepts_var_kw = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values())
        for k, v in call_payload.items():
            if k == "ctx":
                continue
            if accepts_var_kw or k in sig.parameters:
                accepted_kwargs[k] = v
        with execution_frame(workspace_id=self.workspace_id, transport="IPC", method=method_override or "CALL", payload=call_payload):
            raw_res = handler(target_ctx, **accepted_kwargs)
        status_code = 200
        body = raw_res

        if isinstance(raw_res, tuple):
            body = raw_res[0]
            if len(raw_res) > 1 and isinstance(raw_res[1], int):
                status_code = raw_res[1]
        if isinstance(body, Response):
            status_code = body.status_code
            parsed_json = body.get_json(silent=True)
            body = parsed_json if parsed_json is not None else body.get_data(as_text=True)

        if status_code >= 400:
            err_msg = body.get("error") if isinstance(body, dict) else str(body)
            raise RuntimeError(f"Capability '{capability}' failed (HTTP {status_code}): {err_msg}")

        if wait and status_code == 202 and isinstance(body, dict) and body.get("job_id"):
            job_id = body["job_id"]
            settled = target_ctx.jobs.wait_for(job_id=job_id, timeout=timeout)
            if not settled:
                raise TimeoutError(f"Capability '{capability}' timed out waiting for job '{job_id}' ({timeout}s).")
            job_state = target_ctx.jobs.get_status(job_id) or {}
            if job_state.get("status") == "failed":
                raise RuntimeError(job_state.get("message") or f"Background job '{job_id}' failed.")
            artifact = job_state.get("artifact")
            if isinstance(artifact, dict) and artifact:
                return artifact
            return job_state

        return body
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

    def get_registered_capabilities(self) -> Dict[str, Dict[str, Any]]:
        """Returns a shallow copy of the global transport-agnostic capability registry."""
        return get_registered_capabilities()

    def emit_sse(self, event_type: str, payload: Dict[str, Any]) -> None:
        """Publishes a real-time Server-Sent Event onto the global SSE bus."""
        sse_bus.emit_sse(event_type, payload)
    def get_repo_data_path(self, repo: str, subpath: str = "") -> str:
        """Returns the canonical VFS URI for sovereign extension data inside a target repository."""
        ctrl_name = get_control_dir_name()
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
        if not hasattr(self, "_exec_manager") or self._exec_manager is None:
            self._exec_manager = ProcessManager(self.workspace_id)
        return self._exec_manager

    def resolve_path(self, filepath):
        """Safely anchors a relative path to the physical workspace bounds."""
        return _resolve_physical_path(filepath, self.workspace_id)

    def find_path_candidates(self, query_path, **kwargs):
        """Opt-in candidate discovery for path disambiguation."""
        return self.emit_first('find_path_candidates', query_path=query_path, default=[], **kwargs)

    def resolve_dag_chain(self, hook_name: str):
        """SDK Helper: Resolves an ordered compilation/execution DAG chain for the current workspace."""
        return resolve_dag_chain(self, hook_name)

    def parse_uri(self, path_str):
        return parse_uri(path_str)

    def expand_selection(self, items, gather_only=False):
        """
        SSOT: Expands polymorphic frontend selection items into a flat, 
        deduplicated, and stable-sorted list of physical or logical filepaths.
        Enforces a strict List[str] return contract with native VFS directory expansion fallback.
        """
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
                uri_obj = AkasaURI(raw_str)
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
                    normalized_files.append(str(AkasaURI(raw_target)))
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
        _EXTENSION_INSTANCES[self.name] = self

        if self.settings_schema:
            _REGISTERED_SETTINGS_SCHEMAS[self.name] = self.settings_schema
        if self.schema:
            register_schema(self.name, self.schema)
        for db_name, db_schema in self.extra_schemas.items():
            register_schema(db_name, db_schema)
        # Universal REST endpoints for extension settings management
        @self.route('settings', methods=['GET'], docstring="[sync] Retrieves the current settings for this extension.")
        def get_settings(ctx):
            repo = ctx.payload.get('repo')
            return ctx.settings.get_all(repo=repo, mask_secrets=True)
        @self.route('settings', methods=['POST'], response_schema=SettingsUpdateResponse, docstring="[sync] Updates the settings for this extension.")
        def update_settings(ctx):
            try:
                repo = ctx.payload.get('repo') or ctx.payload.get('_repo')
                ctx.settings.update(ctx.payload or {}, repo=repo)
            except Exception as e:
                print(f"Settings Save Error: {traceback.format_exc()}")
                return {"status": "error", "error": f"Internal save error: {str(e)}"}, 500
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
            return payload
        @self.route('settings/schema', methods=['GET'], docstring="[sync] Retrieves the declarative settings schema for this extension.")
        def get_settings_schema(ctx):
            schema = self.settings_schema(ctx.workspace_id) if callable(self.settings_schema) else self.settings_schema
            return schema

    def register_schema(self, db_name: str, schema_dict: Dict[str, Dict[str, str]]) -> None:
        """Registers an additional declarative SQLite schema owned by this extension."""
        register_schema(db_name, schema_dict)
    def get_context(self, workspace_id=None, job_id=None, root_job_id=None, chain_state=None, chain_history=None, payload=None):
        """SDK Factory: Returns a fully hydrated context inherently bound to this extension's schema."""
        wid = workspace_id or sniff_tenant_id() or "default"
        return self.context_class(
            self.name,
            wid,
            settings_schema=self.settings_schema,
            job_id=job_id,
            root_job_id=root_job_id,
            chain_state=chain_state,
            chain_history=chain_history,
            payload=payload,
        )

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
                    wid = kwargs.get('workspace_id') or sniff_tenant_id() or "default"
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
        Injects the ExtensionContext, isolates pipeline envelope state onto ctx.jobs,
        and forwards only accepted parameters to the worker function.
        """
        def decorator(f):
            f_sig = inspect.signature(f)
            f_params = set(f_sig.parameters.keys()) - {"ctx"}
            accepts_var_kw = any(
                p.kind == inspect.Parameter.VAR_KEYWORD for p in f_sig.parameters.values()
            )
            envelope_keys = {"root_job_id", "chain_state", "chain_history", "_workspace_id", "_chain", "_dag_root_id", "_dag_step_id"}
            # Do not use functools.wraps here. The metronome relies on inspect.signature
            # to dynamically inject job_id and workspace_id based on the wrapper's exact kwargs.
            def wrapper(job_id=None, workspace_id=None, **kwargs):
                if isinstance(job_id, ExtensionContext):
                    ctx = job_id
                    actual_job_id = kwargs.pop("job_id", ctx.jobs.current_job_id)
                    actual_ws_id = workspace_id or ctx.workspace_id
                    if "chain_state" in kwargs and isinstance(kwargs["chain_state"], dict):
                        ctx.jobs.chain_state = dict(kwargs["chain_state"])
                    if "chain_history" in kwargs and isinstance(kwargs["chain_history"], list):
                        ctx.jobs.chain_history = list(kwargs["chain_history"])
                    if "root_job_id" in kwargs and kwargs["root_job_id"]:
                        ctx.jobs.root_job_id = kwargs["root_job_id"]
                else:
                    actual_job_id = job_id
                    actual_ws_id = workspace_id
                    root_job_id = kwargs.get("root_job_id") or actual_job_id
                    chain_state = kwargs.get("chain_state")
                    chain_history = kwargs.get("chain_history")
                    ctx = self.get_context(
                        actual_ws_id,
                        job_id=actual_job_id,
                        root_job_id=root_job_id,
                        chain_state=chain_state,
                        chain_history=chain_history,
                    )

                call_kwargs = {}
                for k, v in kwargs.items():
                    if k in envelope_keys and k not in f_params:
                        continue
                    if accepts_var_kw or k in f_params:
                        call_kwargs[k] = v
                if "job_id" in f_params:
                    call_kwargs["job_id"] = actual_job_id
                if "workspace_id" in f_params:
                    call_kwargs["workspace_id"] = actual_ws_id
                return f(ctx, **call_kwargs)
            register_callback(self.name, task_name, wrapper, raw_func=f)
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
        mcp_expose = options.pop('mcp_expose', None)
        for method in options.get('methods', ['GET']):
            m_upper = method.upper()
            req_sch = request_schema.get(m_upper) if isinstance(request_schema, dict) else request_schema
            res_sch = response_schema.get(m_upper) if isinstance(response_schema, dict) else response_schema

            self.bp.route_schemas[full_rule][m_upper] = {
                "request_schema": req_sch,
                "response_schema": res_sch,
                "docstring": docstring,
                "mcp_expose": mcp_expose,
            }
        path_params = [m[1] for m in re.findall(r'<([^>:]+:)?([^>]+)>', clean_rule)]
        cap_action = re.sub(r'/<[^>]+>', '', clean_rule).strip('/').replace('/', '.')
        cap_id = f"{self.name}.{cap_action}" if cap_action else self.name
        methods_list = [m.upper() for m in options.get('methods', ['GET'])]

        def decorator(f):
            existing_spec = _CAPABILITY_REGISTRY.get(cap_id)
            handlers_by_method = dict(existing_spec.get("handlers_by_method", {})) if existing_spec else {}
            for m_up in methods_list:
                handlers_by_method[m_up] = f
            merged_methods = list(dict.fromkeys((existing_spec.get("methods", []) if existing_spec else []) + methods_list))
            merged_req_schema = request_schema or (existing_spec.get("request_schema") if existing_spec else None)
            merged_res_schema = response_schema or (existing_spec.get("response_schema") if existing_spec else None)
            merged_doc = docstring or (existing_spec.get("docstring") if existing_spec else None)
            merged_path_params = list(dict.fromkeys((existing_spec.get("path_params", []) if existing_spec else []) + path_params))
            merged_mcp_expose = (
                bool(mcp_expose)
                if mcp_expose is not None
                else bool(existing_spec.get("mcp_expose", True) if existing_spec else True)
            )

            _CAPABILITY_REGISTRY[cap_id] = {
                "id": cap_id,
                "ext_name": self.name,
                "action": cap_action,
                "rule": full_rule,
                "methods": merged_methods,
                "path_params": merged_path_params,
                "request_schema": merged_req_schema,
                "response_schema": merged_res_schema,
                "docstring": merged_doc,
                "mcp_expose": merged_mcp_expose,
                "core": bool(getattr(self, 'core', False)),
                "handler": f,
                "handlers_by_method": handlers_by_method,
            }
            @functools.wraps(f)
            def wrapper(workspace_id, *args, **kwargs):
                req_payload = _extract_http_request_payload(kwargs)
                http_method = str(request.method or "GET").upper() if has_request_context() else "GET"
                with execution_frame(workspace_id=workspace_id, transport="HTTP", method=http_method, payload=req_payload):
                    ctx = self.get_context(workspace_id, payload=req_payload)
                    result = f(ctx, *args, **kwargs)
                    return _marshal_http_return(result)
            # Use getattr to safely bypass the auth gate for core OS engines
            if not getattr(self, 'core', False):
                wrapper = extension_auth(self.name)(wrapper)
            self.bp.route(full_rule, **options)(wrapper)
            return wrapper
        return decorator


_akasa_utils.extension_auth = extension_auth

_akasa_auth.security_bp = AkasaExtension(
    'security',
    _akasa_auth.__name__,
    title="Security & Encryption",
    description="Local encryption key management.",
    core=True,
    settings_schema=lambda ws: [{
        "id": "master_fernet_key",
        "label": "Master Encryption Key (Fernet)",
        "type": "text",
        "scope": "daemon",
        "secure": False,
        "default": get_master_key().decode('utf-8'),
        "description": "This global key encrypts your secrets.json. It is stored securely in your user home directory. If you move this workspace to another machine, you must copy this key to the new machine."
    }]
)