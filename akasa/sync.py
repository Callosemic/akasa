import time
import json
from akasa.db import get_connection
from akasa.hooks import hooks
from akasa.utils import sniff_tenant_id

KERNEL_BOOT_TS = time.time()
def get_active_pipeline_modules(workspace_id=None):
    """
    Fast, single-query helper returning active_modules, pending_modules, and is_pipeline_active
    without scanning vfs_event_log or firing manifest signature hooks.
    """
    wid = workspace_id or sniff_tenant_id() or "default"
    now = time.time()
    conn = get_connection("workers", workspace_id=wid)
    cutoff_ts = now - 300.0
    active_job_rows = conn.execute(
        "SELECT ext_name, callback_name, args_json FROM immediate_jobs WHERE status IN ('pending', 'processing') AND job_category = 'ui_blocking' AND updated_at > ?",
        (cutoff_ts,)
    ).fetchall()

    active_modules_set = set()
    pending_modules_set = set()
    is_pipeline_active = False
    for r in active_job_rows:
        ext_name = r['ext_name']
        if ext_name:
            active_modules_set.add(ext_name)
        try:
            args = json.loads(r['args_json'] or "{}")
            if '_chain' in args or 'chain_history' in args or args.get('is_pipeline'):
                is_pipeline_active = True
            for step in args.get('_chain', {}).get('steps', []):
                if step.get('ext_name'):
                    pending_modules_set.add(step['ext_name'])
            dag_root = args.get('_dag_root_id')
            if dag_root:
                is_pipeline_active = True
                from akasa.workers import _dag_coordinators as _dc, _dag_lock as _dl
                with _dl:
                    coord = _dc.get(dag_root)
                    if coord:
                        for pid in coord.get("pending", set()):
                            p_ext = coord["step_map"].get(pid, {}).get("ext_name")
                            if p_ext:
                                pending_modules_set.add(p_ext)
        except Exception:
            pass

    active_modules = list(active_modules_set)
    pending_modules = [m for m in pending_modules_set if m not in active_modules_set]
    return {
        "is_pipeline_active": is_pipeline_active,
        "active_modules": active_modules,
        "pending_modules": pending_modules
    }


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
    pipeline_state = get_active_pipeline_modules(wid)
    is_pipeline_active = pipeline_state["is_pipeline_active"]
    active_modules = pipeline_state["active_modules"]
    pending_modules = pipeline_state["pending_modules"]

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
        "is_pipeline_active": is_pipeline_active,
        "active_modules": active_modules,
        "pending_modules": pending_modules,
        "mutations": mutations,
        "signatures": signatures
    }