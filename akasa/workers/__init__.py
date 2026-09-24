from pathlib import Path
import os
import time
import json
import threading
from typing import TypedDict, List, Dict, Any, Optional, Literal
from concurrent.futures import ThreadPoolExecutor
from akasa.db import get_connection
from akasa.hooks import hooks

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
_metronome_thread = None
_shutdown_event = threading.Event()
_callbacks = {}
def register_callback(ext_name, callback_name, func):
    """Extensions register their background functions here to be triggered by the Metronome."""
    _callbacks[f"{ext_name}:{callback_name}"] = func
def _prepare_job_payload(args_json, workspace_id):
    if not workspace_id:
        from akasa.utils import sniff_tenant_id
        workspace_id = sniff_tenant_id()
    workspace_id = workspace_id or "default"
    _init_worker_schema(workspace_id)
    conn = get_connection("workers", workspace_id=workspace_id)
    now = time.time()
    try:
        args_payload = args_json if isinstance(args_json, dict) else json.loads(args_json)
        if "_workspace_id" not in args_payload:
            args_payload["_workspace_id"] = workspace_id
        args_json = json.dumps(args_payload)
    except Exception:
        if isinstance(args_json, dict):
            args_json = json.dumps(args_json)
    return workspace_id, conn, now, args_json
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
        INSERT OR REPLACE 
INTO jobs (id, ext_name, callback_name, interval_ms, jitter_ms, next_run_at, status, args_json)
        VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)
    """, (job_id, ext_name, callback_name, interval_ms, jitter_ms, now, args_json))
    conn.commit()


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
def cancel_job(job_id, workspace_id=None):
    """Cancels and removes a scheduled background job from the Metronome ledger."""
    if not workspace_id:
        from akasa.utils import sniff_tenant_id
        workspace_id = sniff_tenant_id()
    workspace_id = workspace_id or "default"

    conn = get_connection("workers", workspace_id=workspace_id)
    conn.execute("DELETE FROM jobs WHERE id=?", (job_id,))
    conn.commit()
def submit_immediate_job(job_id, ext_name, callback_name, args_json="{}", workspace_id=None, coalesce=False, job_category="system_background"):
    """Drops a task directly into the active ThreadPoolExecutor and logs its lifecycle for UI polling."""
    if _shutdown_event.is_set():
        return False

    workspace_id, conn, now, args_json = _prepare_job_payload(args_json, workspace_id)

    if coalesce:
        existing = conn.execute(
            "SELECT id FROM immediate_jobs WHERE ext_name=? AND callback_name=? AND status IN ('pending', 'processing')",
            (ext_name, callback_name)
        ).fetchone()
        if existing:
            return existing['id']

    # Seed the initial artifact with chain context so the UI knows its place in the pipeline
    initial_artifact = {}
    try:
        parsed_args = json.loads(args_json)
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
    if not _shutdown_event.is_set():
        try:
            _executor.submit(_execute_immediate_job, job_id, ext_name, callback_name, args_json, workspace_id)
        except RuntimeError as e:
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
        job_row = conn.execute("SELECT ext_name, artifact_json, job_category FROM immediate_jobs WHERE id=?", (job_id,)).fetchone()
        ext_name = job_row['ext_name'] if job_row else ''
        job_category = job_row['job_category'] if job_row and 'job_category' in job_row.keys() else 'system_background'

        # Hydrate missing artifact payloads from the DB so SSE broadcasts retain chain_history
        if artifact is None and job_row and job_row['artifact_json']:
            try:
                artifact = json.loads(job_row['artifact_json'])
            except Exception:
                artifact = {}
    except Exception:
        ext_name = ''
        job_category = 'system_background'

    conn.commit()

    try:
        from akasa.events import sse_bus
        from akasa.sync import get_system_deltas

        # Calculate SSOT system state to keep UI dumb
        deltas = get_system_deltas(workspace_id=workspace_id)

        sse_bus.emit_sse('job_progress', {
            "id": job_id,
            "ext_name": ext_name,
            "job_category": job_category,
            "status": status,
            "message": message,
            "artifact": artifact or {},
            "workspace_id": workspace_id,
            "active_modules": deltas.get("active_modules", []),
            "pending_modules": deltas.get("pending_modules", [])
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
    chain_state = dict(kwargs.get('chain_state', {}))
    step_error = None
    step_msg = None
    step_artifact = {}

    if func:
        try:
            import inspect
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
            import traceback
            err_trace = traceback.format_exc()
            print(f"❌ [Worker] Immediate Job {job_id} failed on workspace [{target_ws}]:\n{err_trace}")
            step_error = str(e)
            try:
                from akasa.hooks import hooks
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

    # --- STEP CHAINING LOGIC (ADR 0037 - Resilient Handoff) ---
    next_job_id = None
    if chain_data and chain_data.get('steps') and not step_error:
        next_step = chain_data['steps'][0]
        remaining_steps = chain_data['steps'][1:]

        next_chain = {
            "steps": remaining_steps,
            "on_complete_hook": chain_data.get('on_complete_hook')
        }
        # Generic State Reducer: Accumulates and merges pipeline states automatically
        if step_artifact and 'state_deltas' in step_artifact:
            for k, v in step_artifact['state_deltas'].items():
                if isinstance(v, list):
                    existing = chain_state.get(k, [])
                    if isinstance(existing, list):
                        chain_state[k] = list(dict.fromkeys(existing + v))
                    else:
                        chain_state[k] = v
                elif isinstance(v, dict):
                    existing = chain_state.get(k, {})
                    if isinstance(existing, dict):
                        existing.update(v)
                        chain_state[k] = existing
                    else:
                        chain_state[k] = v
                else:
                    chain_state[k] = v

        next_kwargs = {k: v for k, v in kwargs.items() if k not in ('job_id', 'workspace_id', 'root_job_id', 'chain_history', 'chain_state')}
        next_kwargs['_chain'] = next_chain
        next_kwargs['root_job_id'] = root_job_id
        next_kwargs['chain_history'] = history
        next_kwargs['chain_state'] = chain_state

        # Serialize safely, stringifying any non-serializable objects injected by previous steps
        def safe_serializer(obj):
            return str(obj)
        next_args_json = json.dumps(next_kwargs, default=safe_serializer)
        import uuid
        prefix = next_step['ext_name'][:3].lower()
        next_job_id = f"{prefix}_{uuid.uuid4().hex[:8]}"

        # Inherit job category from parent
        try:
            curr_row = get_connection("workers", workspace_id=target_ws).execute("SELECT job_category FROM immediate_jobs WHERE id=?", (job_id,)).fetchone()
            job_cat = curr_row['job_category'] if curr_row and 'job_category' in curr_row.keys() else 'system_background'
        except Exception:
            job_cat = 'system_background'

        submit_immediate_job(next_job_id, next_step['ext_name'], next_step['worker_name'], next_args_json, workspace_id=target_ws, job_category=job_cat)

        from akasa.hooks import hooks
        hooks.emit_background('job_baton_passed', workspace_id=target_ws, root_job_id=root_job_id, previous_job_id=job_id, next_job_id=next_job_id, ext_name=next_step['ext_name'])

    elif chain_data and chain_data.get('on_complete_hook') and not step_error:
        from akasa.hooks import hooks
        hooks.emit_background(chain_data['on_complete_hook'], workspace_id=target_ws)
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
def register_ephemeral_artifact(filepath, owner, ttl_seconds, workspace_id="default"):
    conn = get_connection("workers", workspace_id=workspace_id)
    now = time.time()
    import uuid
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
            from akasa.vfs import execute_vfs_delete
            if os.path.exists(row['filepath']):
                execute_vfs_delete(workspace_id, row['filepath'])
            conn.execute("DELETE FROM ephemeral_artifacts WHERE id=?", (row['id'],))
        except Exception as e:
            print(f"⚠️ Failed to garbage collect ephemeral artifact {row['filepath']}: {e}")

    # 2. Sweep Old Immediate Jobs (> 24 hours)
    cutoff = now - 86400
    conn.execute("DELETE FROM immediate_jobs WHERE updated_at < ? AND status IN ('completed', 'failed')", (cutoff,))
    conn.commit()

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

    if func:
        try:
            import inspect
            sig = inspect.signature(func)
            if 'workspace_id' in sig.parameters:
                kwargs['workspace_id'] = target_ws
            func(**kwargs)
        except Exception as e:
            import traceback
            print(f"❌ [Worker] Job {job_id} failed on workspace context [{target_ws}]: {e}")
            try:
                from akasa.hooks import hooks
                safe_payload = args_json if (args_json and args_json.strip().startswith('{')) else json.dumps({"raw_args": str(args_json)})
                hooks.emit_background('system_error', source=f"Cron:{ext_name}:{callback_name}", error_type=type(e).__name__, message=str(e), traceback=traceback.format_exc(), payload=safe_payload, workspace_id=target_ws)
            except Exception: pass
    
    if not _shutdown_event.is_set():
        try:
            import random
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
def _metronome_loop():
    """The unified Event Loop dispatcher supporting dynamic multi-tenancy."""
    from akasa.utils import _cwd, load_json_config
    control_dir_name = os.environ.get("AKASA_CONTROL_DIR", ".akasa")
    while not _shutdown_event.is_set():
        try:
            index_path = Path(_cwd).joinpath(control_dir_name, "system.json").as_posix()
            workspace_ids = ["default"]

            if os.path.exists(index_path):
                try:
                    w_data = load_json_config(index_path, {})

                    workspace_ids = list(w_data.get("workspaces", {}).keys())
                    if "default" not in workspace_ids:
                        workspace_ids.append("default")
                except Exception:
                    pass
            now = time.time()
            for ws_id in workspace_ids:
                try:
                    # JIT initialization: instantly prep schemas for newly mounted workspaces
                    _init_worker_schema(ws_id)
                    conn = get_connection("workers", workspace_id=ws_id)
                    cursor = conn.execute("SELECT * FROM jobs WHERE status='pending' AND next_run_at <= ?", (now,))
                    ready_jobs = cursor.fetchall()
                    for row in ready_jobs:
                        conn.execute("UPDATE jobs SET status='running' WHERE id=?", (row['id'],))
                        conn.commit()

                        jitter = row['jitter_ms'] if 'jitter_ms' in row.keys() else 0
                        _executor.submit(_execute_job, row['id'], row['ext_name'], row['callback_name'], row['interval_ms'], jitter, row['args_json'], ws_id)
                except Exception as e:
                    import traceback
                    try:
                        from akasa.hooks import hooks
                        hooks.emit_background('system_error', source="Metronome:InnerLoop", error_type=type(e).__name__, message=str(e), traceback=traceback.format_exc(), payload=json.dumps({"workspace": ws_id}), workspace_id=ws_id)
                    except Exception: pass
        except Exception as e:
            import traceback
            try:
                from akasa.hooks import hooks
                hooks.emit_background('system_error', source="Metronome:OuterLoop", error_type=type(e).__name__, message=str(e), traceback=traceback.format_exc(), payload=json.dumps({}), workspace_id="default")
            except Exception: pass
        time.sleep(1.0) # Tick pace

_INITIALIZED_WORKSPACES = set()
_SCHEMA_INIT_LOCK = threading.Lock()

def _init_worker_schema(workspace_id="default"):
        global _INITIALIZED_WORKSPACES
        with _SCHEMA_INIT_LOCK:
            if workspace_id in _INITIALIZED_WORKSPACES:
                    return

            conn = get_connection("workers", workspace_id=workspace_id)
            conn.execute("""
                    CREATE TABLE IF NOT EXISTS jobs (
                            id TEXT PRIMARY KEY,
                            ext_name TEXT,
                            callback_name TEXT,
                            interval_ms INTEGER,
                            jitter_ms INTEGER DEFAULT 0,
                            next_run_at REAL,
                            status TEXT,
                            args_json TEXT
                    )
            """)
            conn.execute("""
                    CREATE TABLE IF NOT EXISTS ephemeral_artifacts (
                            id TEXT PRIMARY KEY,
                            filepath TEXT,
                            module_owner TEXT,
                            created_at REAL,
                            expires_at REAL
                    )
            """)
            conn.execute("""
                    CREATE TABLE IF NOT EXISTS immediate_jobs (
                            id TEXT PRIMARY KEY,
                            ext_name TEXT,
                            callback_name TEXT,
                            status TEXT,
                            status_message TEXT,
                            artifact_json TEXT,
                            meta_json TEXT,
                            created_at REAL,
                            updated_at REAL,
                            args_json TEXT,
                            job_category TEXT DEFAULT 'system_background'
                    )
            """)
            try:
                    conn.execute("ALTER TABLE immediate_jobs ADD COLUMN job_category TEXT DEFAULT 'system_background'")
                    conn.commit()
            except Exception:
                    pass
            try:
                    conn.execute("ALTER TABLE jobs ADD COLUMN jitter_ms INTEGER DEFAULT 0")
                    conn.execute("ALTER TABLE immediate_jobs ADD COLUMN meta_json TEXT")
                    conn.commit()
            except Exception:
                    pass

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
        global _executor, _metronome_thread
        _executor = ThreadPoolExecutor(max_workers=20)
        from akasa.utils import _cwd, load_json_config
        import os
        control_dir_name = os.environ.get("AKASA_CONTROL_DIR", ".akasa")
        index_path = Path(_cwd).joinpath(control_dir_name, "system.json").as_posix()
        workspace_ids = ["default"]
        if os.path.exists(index_path):
                try:
                        w_data = load_json_config(index_path, {})
                        workspace_ids = list(w_data.get("workspaces", {}).keys())
                        if "default" not in workspace_ids:
                                workspace_ids.append("default")
                except Exception:
                        pass
        for ws_id in workspace_ids:
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
    global _executor
    print("🛑 Suspending Worker Metronome...")
    _shutdown_event.set()
    if _executor:
        _executor.shutdown(wait=False)

    try:
        conn = get_connection("workers")
        conn.execute("UPDATE jobs SET status='pending' WHERE status='running'")
        conn.commit()
    except Exception:
        pass