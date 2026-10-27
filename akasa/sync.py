import time
import json
from akasa.context import sniff_tenant_id
from akasa.hooks import hooks
from akasa.db import get_connection
from akasa.workers import get_active_pipeline_modules

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
    pipeline_state = get_active_pipeline_modules(wid)
    is_pipeline_active = pipeline_state["is_pipeline_active"]
    active_modules = pipeline_state["active_modules"]
    pending_modules = pipeline_state["pending_modules"]

    # 3. Collect Signatures via Event Bus
    signatures = hooks.emit_merge_dict('register_manifest_signatures', workspace_id=wid, since_ts=since_ts)
    return {
        "timestamp": now,
        "backend_boot_ts": KERNEL_BOOT_TS,
        "is_pipeline_active": is_pipeline_active,
        "active_modules": active_modules,
        "pending_modules": pending_modules,
        "mutations": mutations,
        "signatures": signatures
    }