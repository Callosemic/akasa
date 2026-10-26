from pathlib import Path
import os
import time
import json
import uuid
import random
import inspect
import traceback
import threading
import typing
from typing import TypedDict, List, Dict, Any, Optional, Literal, Union
from concurrent.futures import ThreadPoolExecutor
from akasa.db import get_connection, apply_declarative_schema, evict_cached_connection, close_thread_connections, _REGISTERED_SCHEMAS
from akasa.hooks import hooks
from akasa.utils import get_workspace_physics, sniff_tenant_id, get_all_workspace_ids, is_extension_enabled, reclaim_process_memory

class ChainStepSchema(TypedDict, total=False):
    id: str
    ext_name: str
    worker_name: str
    anchor: Literal["source", "body", "sink"]
    order: int
    depends_on: List[str]
class ChainStepArtifact(TypedDict, total=False):
    step_id: str
    ext_name: str
    state_deltas: Optional[Dict[str, Any]]
    next_job_id: Optional[str]
    next_kwargs: Optional[Dict[str, Any]]

def resolve_dag_chain(ctx, hook_name: str) -> List[Dict[str, Any]]:
    """
    Generic Akasa DAG Orchestrator.
    Collects step definitions from a registration hook, resolves boundary anchors
    ("source", "body", "sink"), and performs a cycle-safe topological sort.
    """
    steps = []
    for res in ctx.emit(hook_name):
        if res: steps.extend(res)

    # 1. Isolate non-sink step IDs
    non_sink_ids = {s['id'] for s in steps if s.get('anchor') != 'sink' and not s.get('terminal')}

    # 2. Inject non-sink IDs into sink anchors as dynamic prerequisites
    for s in steps:
        is_sink = s.get('anchor') == 'sink' or s.get('terminal')
        if is_sink:
            existing_deps = set(s.get('depends_on', []))
            s['depends_on'] = list(existing_deps | non_sink_ids)

    # 3. Primary sort by explicit priority order
    steps.sort(key=lambda x: (x.get('order', 50), x['id']))

    # 4. Topological Sort with Cycle Guard
    ordered_steps = []
    visited = set()
    visiting = set()

    def visit(step_id):
        if step_id in visited: return
        if step_id in visiting:
            print(f"⚠️ Circular dependency detected in DAG [{hook_name}] involving [{step_id}]. Bypassing.")
            return
        visiting.add(step_id)
        step = next((s for s in steps if s['id'] == step_id), None)
        if step:
            for dep in step.get('depends_on', []):
                visit(dep)
            visited.add(step_id)
            ordered_steps.append(step)
        visiting.remove(step_id)

    for s in steps:
        visit(s['id'])

    return ordered_steps
_executor = None
_interactive_executor = None
_io_executor = None
_metronome_thread = None
_shutdown_event = threading.Event()
_metronome_wakeup = threading.Condition()
_active_job_events: Dict[tuple, threading.Event] = {}
_active_job_lock = threading.Lock()
_dag_coordinators: Dict[str, Dict[str, Any]] = {}
_dag_lock = threading.Lock()
_callbacks = {}
_callback_specs: Dict[str, Dict[str, Any]] = {}
_ENVELOPE_KEYS = frozenset({
    "_workspace_id",
    "_chain",
    "_dag_root_id",
    "_dag_step_id",
    "root_job_id",
    "chain_history",
    "chain_state",
})


def _check_type_match(val: Any, expected_type: Any) -> bool:
    """Recursively validates a runtime value against a Python type annotation, guarding bool vs int."""
    if expected_type is Any or expected_type is inspect.Parameter.empty:
        return True
    if expected_type is None or expected_type is type(None):
        return val is None

    origin = typing.get_origin(expected_type)
    args = typing.get_args(expected_type)

    if origin is Union:
        return any(_check_type_match(val, arg) for arg in args)

    if origin is Literal:
        return any(val == arg and type(val) is type(arg) for arg in args)

    if expected_type is bool:
        return isinstance(val, bool)

    if expected_type is int:
        return isinstance(val, int) and not isinstance(val, bool)

    if expected_type is float:
        return isinstance(val, (int, float)) and not isinstance(val, bool)

    if expected_type in (str, bytes):
        return isinstance(val, expected_type)

    if origin in (list, List) or expected_type in (list, List):
        if not isinstance(val, list):
            return False
        if args:
            return all(_check_type_match(item, args[0]) for item in val)
        return True
    if origin in (set, tuple) or expected_type in (set, tuple):
        if not isinstance(val, (list, tuple, set)):
            return False
        if args:
            if len(args) == 2 and args[1] is Ellipsis:
                return all(_check_type_match(item, args[0]) for item in val)
            if origin is tuple and Ellipsis not in args:
                val_seq = list(val)
                if len(val_seq) != len(args):
                    return False
                return all(_check_type_match(item, arg_t) for item, arg_t in zip(val_seq, args))
            if len(args) == 1:
                return all(_check_type_match(item, args[0]) for item in val)
        return True

    if origin in (dict, Dict) or expected_type in (dict, Dict):
        if not isinstance(val, dict):
            return False
        if len(args) == 2:
            k_type, v_type = args
            return all(_check_type_match(k, k_type) and _check_type_match(v, v_type) for k, v in val.items())
        return True

    if isinstance(expected_type, type):
        if hasattr(expected_type, "__annotations__") and issubclass(expected_type, dict):
            return isinstance(val, dict)
        return isinstance(val, expected_type)

    return True


def _format_type_name(tp: Any) -> str:
    origin = typing.get_origin(tp)
    if origin is not None:
        return str(tp).replace("typing.", "")
    return getattr(tp, "__name__", str(tp))


def validate_worker_kwargs(ext_name: str, callback_name: str, kwargs: Dict[str, Any], allow_extra: bool = False) -> None:
    """
    Synchronously validates worker keyword arguments against the registered function signature
    and type annotations, and verifies strict JSON serializability.
    """
    clean_kwargs = {k: v for k, v in (kwargs or {}).items() if k not in _ENVELOPE_KEYS}
    try:
        json.dumps(clean_kwargs)
    except TypeError as e:
        raise TypeError(
            f"Worker '{ext_name}:{callback_name}' arguments must be strictly JSON-serializable: {e}"
        ) from e

    key = f"{ext_name}:{callback_name}"
    spec = _callback_specs.get(key)
    if not spec:
        return

    params = spec["params"]
    hints = spec["hints"]
    has_var_kw = spec["has_var_kw"]

    if not allow_extra and not has_var_kw:
        unknown = [k for k in clean_kwargs if k not in params]
        if unknown:
            raise TypeError(
                f"Worker '{key}' got unexpected keyword argument(s): {', '.join( sorted(unknown) )}"
            )

    for arg_name, arg_val in clean_kwargs.items():
        if arg_name in hints:
            expected = hints[arg_name]
            if not _check_type_match(arg_val, expected):
                raise TypeError(
                    f"Worker '{key}' parameter '{arg_name}' expected {_format_type_name(expected)}, "
                    f"got {type(arg_val).__name__}"
                )


def validate_dag_chain_kwargs(steps: List[Dict[str, Any]], kwargs: Dict[str, Any]) -> None:
    """Validates shared pipeline kwargs across all steps in a DAG chain."""
    clean_kwargs = {k: v for k, v in (kwargs or {}).items() if k not in _ENVELOPE_KEYS}
    try:
        json.dumps(clean_kwargs)
    except TypeError as e:
        raise TypeError(f"DAG chain arguments must be strictly JSON-serializable: {e}") from e

    if not steps:
        return

    accepted_union = set()
    any_has_var_kw = False
    has_registered_specs = False

    for s in steps:
        ext_name = s.get("ext_name", "")
        worker_name = s.get("worker_name", "")
        validate_worker_kwargs(ext_name, worker_name, clean_kwargs, allow_extra=True)
        spec = _callback_specs.get(f"{ext_name}:{worker_name}")
        if spec:
            has_registered_specs = True
            accepted_union.update(spec["params"].keys())
            if spec["has_var_kw"]:
                any_has_var_kw = True
        else:
            any_has_var_kw = True

    if has_registered_specs and not any_has_var_kw:
        unknown = [k for k in clean_kwargs if k not in accepted_union]
        if unknown:
            raise TypeError(
                f"DAG chain got unexpected keyword argument(s) not accepted by any step: {', '.join(sorted(unknown))}"
            )


def start_dag_chain(steps: List[Dict[str, Any]], workspace_id: str, on_complete_hook: Optional[str] = None, job_category: str = "system_background", **kwargs) -> Optional[str]:
    """
    Parallel-capable DAG Chain Coordinator.
    Dispatches all root steps concurrently and fans out/in automatically as prerequisites complete.
    """
    if not steps:
        return None

    validate_dag_chain_kwargs(steps, kwargs)

    step_map = {}
    for idx, s in enumerate(steps):
        sid = s.get("id") or f"{s['ext_name']}:{s['worker_name']}:{idx}"
        step_copy = dict(s)
        step_copy["id"] = sid
        step_map[sid] = step_copy

    all_ids = set(step_map.keys())
    deps_map = {}
    for sid, s in step_map.items():
        raw_deps = s.get("depends_on") or []
        valid_deps = {d for d in raw_deps if d in all_ids}
        deps_map[sid] = valid_deps

    # If no explicit depends_on metadata is provided, fall back to sequential order
    if all(len(d) == 0 for d in deps_map.values()) and len(steps) > 1:
        ordered_ids = [s.get("id") or f"{s['ext_name']}:{s['worker_name']}:{i}" for i, s in enumerate(steps)]
        for i in range(1, len(ordered_ids)):
            deps_map[ordered_ids[i]] = {ordered_ids[i - 1]}

    root_job_id = f"chn_{uuid.uuid4().hex[:8]}"
    ready_ids = [sid for sid, d in deps_map.items() if not d]
    if not ready_ids:
        ready_ids = [next(iter(step_map.keys()))]
    pending_ids = set(all_ids) - set(ready_ids)

    ready_job_ids = {
        sid: (root_job_id if idx == 0 else f"{step_map[sid]['ext_name'][:3].lower()}_{uuid.uuid4().hex[:8]}")
        for idx, sid in enumerate(ready_ids)
    }

    base_kwargs = {k: v for k, v in kwargs.items() if k not in ('_chain', '_dag_root_id', '_dag_step_id')}
    with _dag_lock:
        _dag_coordinators[root_job_id] = {
            "workspace_id": workspace_id,
            "created_at": time.time(),
            "step_map": step_map,
            "deps_map": deps_map,
            "pending": pending_ids,
            "in_flight": set(ready_ids),
            "in_flight_jobs": dict(ready_job_ids),
            "completed": set(),
            "failed": False,
            "on_complete_hook": on_complete_hook,
            "job_category": job_category,
            "kwargs": base_kwargs,
            "history": [],
            "chain_state": dict(kwargs.get("chain_state", {}))
        }
    first_dispatched_id = None
    for sid in ready_ids:
        step = step_map[sid]
        jid = ready_job_ids[sid]
        if first_dispatched_id is None:
            first_dispatched_id = jid
        step_kwargs = dict(base_kwargs)
        step_kwargs["_dag_root_id"] = root_job_id
        step_kwargs["_dag_step_id"] = sid
        step_kwargs["root_job_id"] = root_job_id
        step_kwargs["chain_history"] = []
        step_kwargs["chain_state"] = dict(kwargs.get("chain_state", {}))
        args_json = json.dumps(step_kwargs)
        submit_immediate_job(jid, step["ext_name"], step["worker_name"], args_json, workspace_id=workspace_id, coalesce=False, job_category=job_category)

    return first_dispatched_id

def run_parallel(fn, items, max_workers: int = 4) -> list:
    """
    Executes `fn(item)` across `items` using the shared bounded Akasa I/O ThreadPoolExecutor
    and deterministically closes thread-local SQLite connections on each worker thread.
    """
    global _io_executor
    items_list = list(items or [])
    if not items_list:
        return []
    if len(items_list) == 1 or threading.current_thread().name.startswith("Akasa-IO"):
        return [fn(item) for item in items_list]
    if _io_executor is None:
        _io_executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="Akasa-IO")

    def _safe_worker(item):
        try:
            return fn(item)
        finally:
            close_thread_connections()

    futures = [_io_executor.submit(_safe_worker, item) for item in items_list]
    return [f.result() for f in futures]


def wait_for_jobs(workspace_id: str, prefix: str = None, job_id: str = None, timeout: float = 10.0) -> bool:
    """Blocks the calling thread until all matching in-flight immediate jobs complete or timeout elapses."""
    deadline = time.time() + timeout
    while True:
        remaining = deadline - time.time()
        if remaining <= 0:
            return False
        with _active_job_lock:
            matching_events = [
                ev for (ws, jid), ev in _active_job_events.items()
                if ws == workspace_id and ((job_id and jid == job_id) or (prefix and jid.startswith(prefix)))
            ]
        if not matching_events:
            return True
        if not matching_events[0].wait(timeout=min(remaining, 0.5)):
            if time.time() >= deadline:
                return False
def _merge_state_deltas(target_state: Dict[str, Any], deltas: Dict[str, Any]) -> None:
    """Merges step state_deltas into the shared pipeline chain_state dictionary."""
    if not deltas or not isinstance(deltas, dict):
        return
    for k, v in deltas.items():
        if isinstance(v, list):
            existing = target_state.get(k, [])
            target_state[k] = list(dict.fromkeys(existing + v)) if isinstance(existing, list) else v
        elif isinstance(v, dict):
            existing = target_state.get(k, {})
            if isinstance(existing, dict):
                existing.update(v)
                target_state[k] = existing
            else:
                target_state[k] = v
        else:
            target_state[k] = v
def register_callback(ext_name, callback_name, func, raw_func=None):
    """Extensions register their background functions here to be triggered by the Metronome."""
    key = f"{ext_name}:{callback_name}"
    _callbacks[key] = func
    target = raw_func or inspect.unwrap(func)
    try:
        sig = inspect.signature(target)
        params = {}
        has_var_kw = False
        for idx, (p_name, p_obj) in enumerate(sig.parameters.items()):
            if idx == 0 and p_name == "ctx":
                continue
            if p_obj.kind == inspect.Parameter.VAR_KEYWORD:
                has_var_kw = True
            elif p_obj.kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY):
                params[p_name] = p_obj
        try:
            hints = typing.get_type_hints(target)
        except Exception:
            hints = getattr(target, "__annotations__", {}) or {}
        _callback_specs[key] = {
            "params": params,
            "hints": {k: v for k, v in hints.items() if k != "return" and k != "ctx"},
            "has_var_kw": has_var_kw,
            "raw_func": target,
        }
    except Exception:
        pass


def _prepare_job_payload(args_json, workspace_id):
    if not workspace_id:
        workspace_id = sniff_tenant_id()
    workspace_id = workspace_id or "default"
    _init_worker_schema(workspace_id)
    conn = get_connection("workers", workspace_id=workspace_id)
    now = time.time()
    if isinstance(args_json, dict):
        args_payload = dict(args_json)
    else:
        args_payload = json.loads(args_json or "{}")
    if isinstance(args_payload, dict) and "_workspace_id" not in args_payload:
        args_payload["_workspace_id"] = workspace_id
    serialized_json = json.dumps(args_payload)
    return workspace_id, conn, now, serialized_json
def submit_job(job_id, ext_name, callback_name, interval_ms, args_json="{}", jitter_ms=0, workspace_id=None):
    """
    Schedules a recurring background task in the SQLite Ledger.

    Note: `interval_ms` must be > 0 for recurring tasks. If `interval_ms == 0`, 
    the job is treated as a one-shot task and will be deleted after its first execution.
    For clarity, prefer `submit_one_shot_job()` when scheduling one-time delayed tasks.
    """
    if _shutdown_event.is_set():
        return False
    workspace_id, conn, now, args_json = _prepare_job_payload(args_json, workspace_id)
    conn.execute("""
        INSERT OR REPLACE INTO jobs (id, ext_name, callback_name, interval_ms, jitter_ms, next_run_at, status, args_json)
        VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)
    """, (job_id, ext_name, callback_name, interval_ms, jitter_ms, now, args_json))
    conn.commit()
    with _metronome_wakeup:
        _metronome_wakeup.notify_all()


def submit_one_shot_job(job_id, ext_name, callback_name, delay_ms, args_json="{}", workspace_id=None):
    """
    Schedules a one-time delayed task.
    The task will execute once when `delay_ms` elapses and will be automatically deleted from the ledger.
    """
    if _shutdown_event.is_set():
        return False

    now = time.time()
    next_run = now + (delay_ms / 1000.0)

    workspace_id, conn, _, args_json = _prepare_job_payload(args_json, workspace_id)
    conn.execute("""
        INSERT OR REPLACE 
        INTO jobs (id, ext_name, callback_name, interval_ms, jitter_ms, next_run_at, status, args_json)
        VALUES (?, ?, ?, 0, 0, ?, 'pending', ?)
    """, (job_id, ext_name, callback_name, next_run, args_json))
    conn.commit()
    with _metronome_wakeup:
        _metronome_wakeup.notify_all()
def cancel_job(job_id, workspace_id=None):
    """Cancels and removes a scheduled background job from the Metronome ledger."""
    if not workspace_id:
        workspace_id = sniff_tenant_id()
    workspace_id = workspace_id or "default"
    conn = get_connection("workers", workspace_id=workspace_id)
    conn.execute("DELETE FROM jobs WHERE id=?", (job_id,))
    conn.commit()

def pause_job(job_id, workspace_id=None):
    """Freezes a scheduled job in the Metronome ledger without deleting its record."""
    if not workspace_id:
        workspace_id = sniff_tenant_id()
    workspace_id = workspace_id or "default"

    conn = get_connection("workers", workspace_id=workspace_id)
    cursor = conn.execute("UPDATE jobs SET status='paused' WHERE id=?", (job_id,))
    conn.commit()
    return cursor.rowcount > 0

def resume_job(job_id, workspace_id=None, reset_timer=True):
    """Restores a paused job to pending status, optionally recalculating next_run_at."""
    if not workspace_id:
        workspace_id = sniff_tenant_id()
    workspace_id = workspace_id or "default"

    conn = get_connection("workers", workspace_id=workspace_id)
    if reset_timer:
        row = conn.execute("SELECT interval_ms FROM jobs WHERE id=?", (job_id,)).fetchone()
        if not row:
            return False
        interval_ms = row["interval_ms"] or 0
        next_run = time.time() + (interval_ms / 1000.0)
        cursor = conn.execute(
            "UPDATE jobs SET status='pending', next_run_at=? WHERE id=?",
            (next_run, job_id)
        )
    else:
        cursor = conn.execute("UPDATE jobs SET status='pending' WHERE id=?", (job_id,))
    conn.commit()
    return cursor.rowcount > 0
def submit_immediate_job(job_id, ext_name, callback_name, args_json="{}", workspace_id=None, coalesce=False, job_category="system_background"):
    """Drops a task directly into the active ThreadPoolExecutor and logs its lifecycle for UI polling."""
    if _shutdown_event.is_set():
        return False

    workspace_id, conn, now, args_json = _prepare_job_payload(args_json, workspace_id)
    if coalesce == "args":
        existing = conn.execute(
            "SELECT id FROM immediate_jobs WHERE ext_name=? AND callback_name=? AND status IN ('pending', 'processing') AND args_json=?",
            (ext_name, callback_name, args_json)
        ).fetchone()
        if existing:
            return existing['id']
    elif coalesce:
        existing = conn.execute(
            "SELECT id FROM immediate_jobs WHERE ext_name=? AND callback_name=? AND status IN ('pending', 'processing')",
            (ext_name, callback_name)
        ).fetchone()
        if existing:
            return existing['id']
    # Seed the initial artifact with chain context so the UI knows its place in the pipeline
    initial_artifact = {}
    is_dag_or_chain = False
    try:
        parsed_args = json.loads(args_json)
        if isinstance(parsed_args, dict):
            if '_dag_root_id' in parsed_args or '_chain' in parsed_args:
                is_dag_or_chain = True
            if 'chain_history' in parsed_args:
                initial_artifact['chain_history'] = parsed_args['chain_history']
            if 'root_job_id' in parsed_args:
                initial_artifact['root_job_id'] = parsed_args['root_job_id']
    except Exception:
        pass
    conn.execute("""
        INSERT OR REPLACE INTO immediate_jobs (id, ext_name, callback_name, status, status_message, artifact_json, created_at, updated_at, args_json, job_category)
        VALUES (?, ?, ?, 'processing', 'Initializing...', ?, ?, ?, ?, ?)
    """, (job_id, ext_name, callback_name, json.dumps(initial_artifact), now, now, args_json, job_category))
    conn.commit()
    with _active_job_lock:
        _active_job_events[(workspace_id, job_id)] = threading.Event()
    if not _shutdown_event.is_set():
        if _executor is None or _interactive_executor is None:
            start_workers()
        target_pool = _interactive_executor if (not is_dag_or_chain and job_category == "ui_blocking") else _executor
        try:
            target_pool.submit(_execute_immediate_job, job_id, ext_name, callback_name, args_json, workspace_id)
        except RuntimeError as e:
            with _active_job_lock:
                ev = _active_job_events.pop((workspace_id, job_id), None)
                if ev:
                    ev.set()
            if "shutdown" not in str(e).lower():
                raise
    return job_id
def update_immediate_job_meta(job_id, meta_dict, workspace_id="default"):
    """Helper for workers to safely update discrete metrics without altering stream status."""
    conn = get_connection("workers", workspace_id=workspace_id)
    conn.execute("UPDATE immediate_jobs SET meta_json=? WHERE id=?", (json.dumps(meta_dict), job_id))
    conn.commit()

def update_immediate_job_status(job_id, status, message=None, artifact=None, workspace_id="default"):
    """Helper for workers to update their streaming status."""
    conn = get_connection("workers", workspace_id=workspace_id)
    now = time.time()

    updates = ["updated_at=?"]
    params = [now]

    if status is not None:
        updates.append("status=?")
        params.append(status)
    if message is not None:
        updates.append("status_message=?")
        params.append(message)
    if artifact is not None:
        updates.append("artifact_json=?")
        params.append(json.dumps(artifact))

    params.append(job_id)
    query = f"UPDATE immediate_jobs SET {', '.join(updates)} WHERE id=?"
    conn.execute(query, tuple(params))
    try:
        if artifact is not None:
            job_row = conn.execute("SELECT ext_name, job_category FROM immediate_jobs WHERE id=?", (job_id,)).fetchone()
        else:
            job_row = conn.execute("SELECT ext_name, artifact_json, job_category FROM immediate_jobs WHERE id=?", (job_id,)).fetchone()
            if job_row and job_row['artifact_json']:
                try:
                    artifact = json.loads(job_row['artifact_json'])
                except Exception:
                    artifact = {}
        ext_name = job_row['ext_name'] if job_row else ''
        job_category = job_row['job_category'] if job_row and 'job_category' in job_row.keys() else 'system_background'
    except Exception:
        ext_name = ''
        job_category = 'system_background'

    conn.commit()

    try:
        from akasa.events import sse_bus as _sse_bus
        from akasa.sync import get_active_pipeline_modules as _get_active_pipeline_modules

        pipeline_state = _get_active_pipeline_modules(workspace_id=workspace_id)

        _sse_bus.emit_sse('job_progress', {
            "id": job_id,
            "job_id": job_id,
            "ext_name": ext_name,
            "job_category": job_category,
            "status": status,
            "message": message,
            "artifact": artifact or {},
            "workspace_id": workspace_id,
            "active_modules": pipeline_state.get("active_modules", []),
            "pending_modules": pipeline_state.get("pending_modules", [])
        })
    except Exception:
        pass

def _execute_immediate_job(job_id, ext_name, callback_name, args_json, target_ws):
    """Executes an immediate job and manages its lifecycle in the ledger."""
    func = _callbacks.get(f"{ext_name}:{callback_name}")
    kwargs = {}
    if args_json:
        try:
            parsed = args_json if isinstance(args_json, dict) else json.loads(args_json)
            if isinstance(parsed, dict):
                kwargs = parsed
                if "_workspace_id" in kwargs:
                    target_ws = kwargs.pop("_workspace_id")
        except Exception as e:
            print(f"⚠️ [Worker] Failed to parse args_json for {job_id}: {e}")
    root_job_id = kwargs.get('root_job_id') or job_id
    history = list(kwargs.get('chain_history', []))
    chain_data = kwargs.pop('_chain', None)
    dag_root_id = kwargs.pop('_dag_root_id', None)
    dag_step_id = kwargs.pop('_dag_step_id', None)
    chain_state = dict(kwargs.get('chain_state', {}))
    step_error = None
    step_msg = None
    step_artifact = {}

    if not is_extension_enabled(ext_name, target_ws):
        step_error = f"Extension '{ext_name}' is disabled in workspace '{target_ws}'."
        print(f"⚠️ [Worker] {step_error}")
    elif func:
        try:
            sig = inspect.signature(func)
            if 'workspace_id' in sig.parameters:
                kwargs['workspace_id'] = target_ws
            if 'job_id' in sig.parameters:
                kwargs['job_id'] = job_id

            result = func(**kwargs)
            if isinstance(result, str):
                step_msg = result
            elif isinstance(result, dict):
                step_msg = result.get('message')
                step_artifact = result.get('artifact', {})
                # Allows the current step to inject data into the downstream pipeline
                if 'next_kwargs' in result:
                    kwargs.update(result['next_kwargs'])
        except Exception as e:
            err_trace = traceback.format_exc()
            print(f"❌ [Worker] Immediate Job {job_id} failed on workspace [{target_ws}]:\n{err_trace}")
            step_error = str(e)
            try:
                # args_json might not be valid JSON if it failed parsing
                safe_payload = args_json if (args_json and args_json.strip().startswith('{')) else json.dumps({"raw_args": str(args_json)})
                hooks.emit_background('system_error', source=f"Worker:{ext_name}:{callback_name}", error_type=type(e).__name__, message=str(e), traceback=err_trace, payload=safe_payload, workspace_id=target_ws)
            except Exception: pass
    else:
        step_error = f"Callback {ext_name}:{callback_name} not found."
        print(f"❌ [Worker] {step_error}")

    history.append({
        "ext_name": ext_name,
        "job_id": job_id,
        "status": "failed" if step_error else "completed",
        "message": step_error or step_msg or "Step complete."
    })
    # --- STEP CHAINING LOGIC (ADR 0037 & Parallel DAG Fan-Out/Fan-In) ---
    next_job_id = None
    if dag_root_id and dag_step_id:
        to_dispatch = []
        should_fire_complete = False
        complete_hook = None
        job_cat = 'system_background'
        with _dag_lock:
            coord = _dag_coordinators.get(dag_root_id)
            if coord:
                coord["in_flight"].discard(dag_step_id)
                coord.get("in_flight_jobs", {}).pop(dag_step_id, None)
                coord["history"].append({
                    "ext_name": ext_name,
                    "job_id": job_id,
                    "status": "failed" if step_error else "completed",
                    "message": step_error or step_msg or "Step complete."
                })
                history = list(coord["history"])
                job_cat = coord.get("job_category", "system_background")
                if step_error:
                    coord["failed"] = True
                    coord["error_message"] = step_error
                    coord["pending"].clear()
                elif coord.get("failed"):
                    coord["pending"].clear()
                else:
                    coord["completed"].add(dag_step_id)
                    if step_artifact and 'state_deltas' in step_artifact:
                        _merge_state_deltas(coord["chain_state"], step_artifact['state_deltas'])

                    for k, v in kwargs.items():
                        if k not in ('job_id', 'workspace_id', 'root_job_id', 'chain_history', 'chain_state'):
                            coord["kwargs"][k] = v

                    newly_ready = [
                        pid for pid in list(coord["pending"])
                        if coord["deps_map"].get(pid, set()).issubset(coord["completed"])
                    ]
                    for pid in newly_ready:
                        coord["pending"].remove(pid)
                        coord["in_flight"].add(pid)
                        step_def = coord["step_map"][pid]
                        jid = f"{step_def['ext_name'][:3].lower()}_{uuid.uuid4().hex[:8]}"
                        coord.setdefault("in_flight_jobs", {})[pid] = jid
                        step_kw = dict(coord["kwargs"])
                        step_kw["_dag_root_id"] = dag_root_id
                        step_kw["_dag_step_id"] = pid
                        step_kw["root_job_id"] = dag_root_id
                        step_kw["chain_history"] = list(coord["history"])
                        step_kw["chain_state"] = dict(coord["chain_state"])
                        to_dispatch.append((jid, step_def, step_kw))
                if to_dispatch:
                    next_job_id = to_dispatch[0][0]
                elif coord["in_flight"]:
                    # Sibling branch is still running; hand baton to the active sibling job ID
                    active_sibling_ids = list(coord.get("in_flight_jobs", {}).values())
                    next_job_id = active_sibling_ids[0] if active_sibling_ids else dag_root_id
                else:
                    if not coord["failed"] and coord.get("on_complete_hook"):
                        should_fire_complete = True
                        complete_hook = coord["on_complete_hook"]
                    _dag_coordinators.pop(dag_root_id, None)

        for jid, step_def, step_kw in to_dispatch:
            validate_worker_kwargs(step_def['ext_name'], step_def['worker_name'], step_kw, allow_extra=True)
            next_args_json = json.dumps(step_kw)
            submit_immediate_job(jid, step_def['ext_name'], step_def['worker_name'], next_args_json, workspace_id=target_ws, job_category=job_cat)
            hooks.emit_background('job_baton_passed', workspace_id=target_ws, root_job_id=dag_root_id, previous_job_id=job_id, next_job_id=jid, ext_name=step_def['ext_name'])
        if should_fire_complete and complete_hook:
            hooks.emit_background(complete_hook, workspace_id=target_ws)
            reclaim_process_memory()

    elif chain_data and chain_data.get('steps') and not step_error:
        next_step = chain_data['steps'][0]
        remaining_steps = chain_data['steps'][1:]

        next_chain = {
            "steps": remaining_steps,
            "on_complete_hook": chain_data.get('on_complete_hook')
        }
        # Generic State Reducer: Accumulates and merges pipeline states automatically
        if step_artifact and 'state_deltas' in step_artifact:
            _merge_state_deltas(chain_state, step_artifact['state_deltas'])

        next_kwargs = {k: v for k, v in kwargs.items() if k not in ('job_id', 'workspace_id', 'root_job_id', 'chain_history', 'chain_state')}
        next_kwargs['_chain'] = next_chain
        next_kwargs['root_job_id'] = root_job_id
        next_kwargs['chain_history'] = history
        next_kwargs['chain_state'] = chain_state
        validate_worker_kwargs(next_step['ext_name'], next_step['worker_name'], next_kwargs, allow_extra=True)
        next_args_json = json.dumps(next_kwargs)
        prefix = next_step['ext_name'][:3].lower()
        next_job_id = f"{prefix}_{uuid.uuid4().hex[:8]}"

        # Inherit job category from parent
        try:
            curr_row = get_connection("workers", workspace_id=target_ws).execute("SELECT job_category FROM immediate_jobs WHERE id=?", (job_id,)).fetchone()
            job_cat = curr_row['job_category'] if curr_row and 'job_category' in curr_row.keys() else 'system_background'
        except Exception:
            job_cat = 'system_background'

        submit_immediate_job(next_job_id, next_step['ext_name'], next_step['worker_name'], next_args_json, workspace_id=target_ws, job_category=job_cat)

        hooks.emit_background('job_baton_passed', workspace_id=target_ws, root_job_id=root_job_id, previous_job_id=job_id, next_job_id=next_job_id, ext_name=next_step['ext_name'])
    elif chain_data and chain_data.get('on_complete_hook') and not step_error:
        hooks.emit_background(chain_data['on_complete_hook'], workspace_id=target_ws)
        reclaim_process_memory()
    # Final completion/failure evaluation for current step
    conn = get_connection("workers", workspace_id=target_ws)
    current = conn.execute("SELECT status, artifact_json, status_message FROM immediate_jobs WHERE id=?", (job_id,)).fetchone()
    if current:
        artifact = json.loads(current['artifact_json']) if current['artifact_json'] else {}
        # Mandatory Envelope Normalization: ChainStepArtifact Contract
        normalized_artifact = {
            "step_id": callback_name,
            "ext_name": ext_name,
            "state_deltas": step_artifact.get("state_deltas", {}) if step_artifact else {}
        }

        artifact.update(normalized_artifact)
        if step_artifact:
            for k, v in step_artifact.items():
                if k not in normalized_artifact:
                    artifact[k] = v
        artifact['root_job_id'] = root_job_id
        artifact['chain_history'] = history
        if next_job_id:
            artifact['next_job_id'] = next_job_id
        msg = step_msg or current['status_message']
        if step_error:
            update_immediate_job_status(job_id, 'failed', f"Error: {step_error}", artifact, target_ws)
        else:
            update_immediate_job_status(job_id, 'completed', msg, artifact, target_ws)
    with _active_job_lock:
        ev = _active_job_events.pop((target_ws, job_id), None)
        if ev:
            ev.set()
    close_thread_connections()
def register_ephemeral_artifact(filepath, owner, ttl_seconds, workspace_id="default"):
    conn = get_connection("workers", workspace_id=workspace_id)
    now = time.time()
    art_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO ephemeral_artifacts (id, filepath, module_owner, created_at, expires_at) VALUES (?, ?, ?, ?, ?)",
        (art_id, filepath, owner, now, now + ttl_seconds)
    )
    conn.commit()

def sweep_ephemeral_artifacts(workspace_id="default"):
    """Sweeps and deletes expired temporary files and old immediate jobs."""
    conn = get_connection("workers", workspace_id=workspace_id)
    now = time.time()
    # 1. Sweep Ephemeral Artifacts
    cursor = conn.execute("SELECT id, filepath FROM ephemeral_artifacts WHERE expires_at < ?", (now,))
    for row in cursor.fetchall():
        try:
            from akasa.vfs import execute_vfs_delete as _execute_vfs_delete
            if os.path.exists(row['filepath']):
                _execute_vfs_delete(workspace_id, row['filepath'])
            conn.execute("DELETE FROM ephemeral_artifacts WHERE id=?", (row['id'],))
        except Exception as e:
            print(f"⚠️ Failed to garbage collect ephemeral artifact {row['filepath']}: {e}")
    # 2. Sweep Old Immediate Jobs, VFS Event Logs (> 24 hours), & Orphaned DAG Coordinators (> 1 hour)
    cutoff = now - 86400
    conn.execute("DELETE FROM immediate_jobs WHERE updated_at < ? AND status IN ('completed', 'failed')", (cutoff,))
    conn.execute("DELETE FROM vfs_event_log WHERE timestamp < ?", (cutoff,))
    conn.commit()
    with _dag_lock:
        stale_dags = [k for k, v in _dag_coordinators.items() if now - v.get("created_at", now) > 3600]
        for k in stale_dags:
            _dag_coordinators.pop(k, None)

    # 3. Sweep Old Database Corruption Dumps (> 7 days)
    try:
        cfg_path, _ = get_workspace_physics(workspace_id)
        db_base = Path(cfg_path).parent.joinpath("cache", "db")
        if db_base.exists() and db_base.is_dir():
            seven_days_ago = now - (7 * 86400)
            for corrupt_file in db_base.rglob("*.db.corrupt_*"):
                if corrupt_file.is_file() and corrupt_file.stat().st_mtime < seven_days_ago:
                    os.remove(corrupt_file.as_posix())
    except Exception as e:
        print(f"⚠️ Failed to garbage collect old corrupt database dumps: {e}")

    reclaim_process_memory()

register_callback("workers", "sweep_ephemeral_artifacts", sweep_ephemeral_artifacts)

def _execute_job(job_id, ext_name, callback_name, interval_ms, jitter_ms, args_json, fallback_ws="default"):
    """Executes the mapped Python callback inside the ThreadPool ensuring contextual tenant forwarding."""
    func = _callbacks.get(f"{ext_name}:{callback_name}")
    target_ws = fallback_ws
    try:
        kwargs = args_json if isinstance(args_json, dict) else json.loads(args_json)
        if "_workspace_id" in kwargs:
            target_ws = kwargs.pop("_workspace_id")
    except Exception:
        kwargs = {}
    if not is_extension_enabled(ext_name, target_ws):
        print(f"⚠️ [Worker] Extension '{ext_name}' is disabled in workspace '{target_ws}'. Dropping recurring job '{job_id}'.")
        conn = get_connection("workers", workspace_id=target_ws)
        conn.execute("DELETE FROM jobs WHERE id=?", (job_id,))
        conn.commit()
        close_thread_connections()
        return
    if func:
        try:
            sig = inspect.signature(func)
            if 'workspace_id' in sig.parameters:
                kwargs['workspace_id'] = target_ws
            if 'job_id' in sig.parameters and 'job_id' not in kwargs:
                kwargs['job_id'] = job_id
            func(**kwargs)
        except Exception as e:
            print(f"❌ [Worker] Job {job_id} failed on workspace context [{target_ws}]: {e}")
            try:
                safe_payload = args_json if (args_json and args_json.strip().startswith('{')) else json.dumps({"raw_args": str(args_json)})
                hooks.emit_background('system_error', source=f"Cron:{ext_name}:{callback_name}", error_type=type(e).__name__, message=str(e), traceback=traceback.format_exc(), payload=safe_payload, workspace_id=target_ws)
            except Exception: pass
    else:
        print(f"⚠️ [Worker] Callback '{ext_name}:{callback_name}' not found for job {job_id}. Suspending job.")
        interval_ms = 0 # Force job deletion

    if not _shutdown_event.is_set():
        try:
            actual_interval = interval_ms
            if jitter_ms and jitter_ms > 0:
                actual_interval += random.randint(-jitter_ms, jitter_ms)
            conn = get_connection("workers", workspace_id=target_ws)
            if interval_ms > 0:
                next_run = time.time() + (actual_interval / 1000.0)
                conn.execute("UPDATE jobs SET status='pending', next_run_at=? WHERE id=?", (next_run, job_id))
            else:
                conn.execute("DELETE FROM jobs WHERE id=?", (job_id,))
            conn.commit()
        except Exception:
            pass
    close_thread_connections()
def _metronome_loop():
    """The unified Event Loop dispatcher supporting dynamic multi-tenancy with condition-variable sleep."""
    while not _shutdown_event.is_set():
        next_wake_delay = 5.0
        try:
            workspace_ids = get_all_workspace_ids()
            now = time.time()
            for ws_id in workspace_ids:
                if _shutdown_event.is_set():
                    break
                try:
                    # JIT initialization: instantly prep schemas for newly mounted workspaces
                    _init_worker_schema(ws_id)
                    conn = get_connection("workers", workspace_id=ws_id)
                    cursor = conn.execute("SELECT * FROM jobs WHERE status='pending' AND next_run_at <= ?", (now,))
                    ready_jobs = cursor.fetchall()
                    for row in ready_jobs:
                        executor = _executor
                        if _shutdown_event.is_set() or executor is None:
                            break
                        conn.execute("UPDATE jobs SET status='running' WHERE id=?", (row['id'],))
                        conn.commit()

                        jitter = row['jitter_ms'] if 'jitter_ms' in row.keys() else 0
                        try:
                            executor.submit(_execute_job, row['id'], row['ext_name'], row['callback_name'], row['interval_ms'], jitter, row['args_json'], ws_id)
                        except RuntimeError as sub_err:
                            if "shutdown" in str(sub_err).lower():
                                conn.execute("UPDATE jobs SET status='pending' WHERE id=?", (row['id'],))
                                conn.commit()
                                break
                            raise

                    next_row = conn.execute("SELECT MIN(next_run_at) as min_ts FROM jobs WHERE status='pending'").fetchone()
                    if next_row and next_row['min_ts'] is not None:
                        wait_sec = max(0.05, next_row['min_ts'] - time.time())
                        next_wake_delay = min(next_wake_delay, wait_sec)
                except Exception as e:
                    if _shutdown_event.is_set() or "cannot schedule new futures after shutdown" in str(e).lower():
                        break
                    try:
                        if "malformed" in str(e).lower() or "corrupt" in str(e).lower():
                            evict_cached_connection(ws_id, "workers")
                        hooks.emit_background('system_error', source="Metronome:InnerLoop", error_type=type(e).__name__, message=str(e), traceback=traceback.format_exc(), payload=json.dumps({"workspace": ws_id}), workspace_id=ws_id)
                    except Exception: pass
        except Exception as e:
            try:
                hooks.emit_background('system_error', source="Metronome:OuterLoop", error_type=type(e).__name__, message=str(e), traceback=traceback.format_exc(), payload=json.dumps({}), workspace_id="default")
            except Exception: pass

        with _metronome_wakeup:
            if not _shutdown_event.is_set():
                _metronome_wakeup.wait(timeout=next_wake_delay)

_INITIALIZED_WORKSPACES = set()
_SCHEMA_INIT_LOCK = threading.Lock()

@hooks.on('workspace_shutdown')
def _evict_initialized_workspace(workspace_id=None, **kwargs):
    if workspace_id:
        with _SCHEMA_INIT_LOCK:
            _INITIALIZED_WORKSPACES.discard(workspace_id)
def _init_worker_schema(workspace_id="default"):
    global _INITIALIZED_WORKSPACES
    with _SCHEMA_INIT_LOCK:
        if workspace_id in _INITIALIZED_WORKSPACES:
            return

        if 'workers' in _REGISTERED_SCHEMAS:
            apply_declarative_schema('workers', _REGISTERED_SCHEMAS['workers'], workspace_id)

        conn = get_connection("workers", workspace_id=workspace_id)
        try:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_vfs_event_log_ts ON vfs_event_log(timestamp)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_immediate_jobs_status ON immediate_jobs(status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status)")
            conn.commit()
        except Exception:
            pass

        # SQLite Boot Concurrency Guardrail
        for _ in range(5):
            try:
                conn.execute("UPDATE jobs SET status='pending' WHERE status='running'")
                conn.execute("""
                    INSERT OR REPLACE INTO jobs (id, ext_name, callback_name, interval_ms, jitter_ms, next_run_at, status, args_json)
                    VALUES ('sys_garbage_collector', 'workers', 'sweep_ephemeral_artifacts', 300000, 10000, 0, 'pending', '{}')
                """)
                conn.commit()
                break
            except Exception as e:
                if "locked" in str(e).lower():
                    time.sleep(0.2)
                else:
                    break

        _INITIALIZED_WORKSPACES.add(workspace_id)
@hooks.on('system_boot', priority=10)
def start_workers(**kwargs):
    global _executor, _interactive_executor, _metronome_thread
    if _executor is not None and _interactive_executor is not None and _metronome_thread is not None and _metronome_thread.is_alive():
        return
    if _executor is None:
        _executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="Akasa-Worker")
    if _interactive_executor is None:
        _interactive_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="Akasa-Interactive")
    for ws_id in get_all_workspace_ids():
        _init_worker_schema(ws_id)
        # Clean up ghost jobs strictly once during the OS boot sequence
        conn = get_connection("workers", workspace_id=ws_id)
        for _ in range(5):
            try:
                conn.execute("UPDATE immediate_jobs SET status='failed', status_message='Interrupted by system reboot.' WHERE status IN ('pending', 'processing')")
                conn.commit()
                break
            except Exception as e:
                if "locked" in str(e).lower():
                    time.sleep(0.2)
                else:
                    break
    _shutdown_event.clear()
    _metronome_thread = threading.Thread(target=_metronome_loop, daemon=True)
    _metronome_thread.start()
    print("⚙️  Stateless Worker Metronome Online.")
@hooks.on('system_shutdown')
def stop_workers(**kwargs):
    global _executor, _interactive_executor, _io_executor
    print("🛑 Suspending Worker Metronome...")
    _shutdown_event.set()
    with _metronome_wakeup:
        _metronome_wakeup.notify_all()
    if _executor:
        _executor.shutdown(wait=False)
        _executor = None
    if _interactive_executor:
        _interactive_executor.shutdown(wait=False)
        _interactive_executor = None
    if _io_executor:
        _io_executor.shutdown(wait=False)
        _io_executor = None

    try:
        conn = get_connection("workers")
        conn.execute("UPDATE jobs SET status='pending' WHERE status='running'")
        conn.commit()
    except Exception:
        pass