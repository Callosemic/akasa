import time
from akasa.db import get_connection
from akasa.hooks import hooks
from akasa.utils import sniff_tenant_id

KERNEL_BOOT_TS = time.time()

def get_system_deltas(workspace_id=None, since_ts=0.0):
    """
    Calculates lightweight system deltas and collects domain-specific signatures.
    Acts as the low-level kernel heartbeat substrate.
    """
    wid = workspace_id or sniff_tenant_id() or "default"
    now = time.time()
    conn = get_connection("workers", workspace_id=wid)
    
    # 1. Physical Mutations
    cursor = conn.execute(
        "SELECT filepath, mutation_type as operation, timestamp FROM vfs_event_log WHERE timestamp > ? ORDER BY timestamp ASC",
        (since_ts,)
    )
    mutations = [dict(r) for r in cursor.fetchall()]
    # 2. Telemetry & Is Compiling State
    active_job_rows = conn.execute(
        "SELECT ext_name, callback_name, args_json FROM immediate_jobs WHERE status IN ('pending', 'processing')"
    ).fetchall()

    active_modules_set = set()
    pending_modules_set = set()
    is_compiling_immediate = False
    for r in active_job_rows:
        cb = r['callback_name'] or ""
        # Inversion of Control: Let the host application define which tasks block the UI
        is_compiler = any(hooks.emit('evaluate_blocking_task', task_name=cb, workspace_id=wid))

        if is_compiler:
            is_compiling_immediate = True
            if r['ext_name']:
                active_modules_set.add(r['ext_name'])
            try:
                import json
                args = json.loads(r['args_json'] or "{}")
                for step in args.get('_chain', {}).get('steps', []):
                    if step.get('ext_name'):
                        pending_modules_set.add(step['ext_name'])
            except Exception:
                pass

    active_modules = list(active_modules_set)
    pending_modules = [m for m in pending_modules_set if m not in active_modules_set]
    # IoC: Host application determines if any running metronome jobs should lock the UI
    active_jobs = conn.execute("SELECT callback_name FROM jobs WHERE status = 'running'").fetchall()
    is_compiling_delayed = any(
        any(hooks.emit('evaluate_blocking_task', task_name=j['callback_name'], workspace_id=wid))
        for j in active_jobs
    )

    is_compiling = is_compiling_immediate or is_compiling_delayed

    # 3. Collect Signatures via Event Bus
    signatures = {}
    hook_responses = hooks.emit('register_manifest_signatures', workspace_id=wid, since_ts=since_ts)
    for res in hook_responses:
        if isinstance(res, dict):
            for domain, sigs in res.items():
                if domain not in signatures:
                    signatures[domain] = {}
                if isinstance(sigs, dict):
                    signatures[domain].update(sigs)
    return {
        "timestamp": now,
        "backend_boot_ts": KERNEL_BOOT_TS,
        "is_compiling": is_compiling,
        "active_modules": active_modules,
        "pending_modules": pending_modules,
        "mutations": mutations,
        "signatures": signatures
    }