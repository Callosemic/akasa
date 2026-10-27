"""
akasa.utils
~~~~~~~~~~~~~~~~~~~
Unified Ring 2 compatibility facade re-exporting Ring 0 (akasa.context) and Ring 2 (akasa.state) primitives.
"""

import akasa.context as _akasa_context
from akasa.context import (
    ExecutionFrame,
    execution_frame,
    get_current_frame,
    set_tenant_resolver,
    resolve_active_tenant,
    sniff_tenant_id,
    get_control_dir_name,
    get_daemon_cwd,
    set_daemon_cwd,
    register_trusted_host_suffix,
    get_trusted_host_suffixes,
    reclaim_process_memory,
    is_port_listening,
    can_bind_port,
    allocate_ephemeral_port,
    atomic_write_bytes,
    atomic_write_text,
    clone_json_value,
    thread_safe_cache,
    typeddict_to_openapi,
    generate_idempotency_hash,
    parse_uri,
    fold_vfs_events,
    slugify,
    is_subpath,
    build_tree_dict,
    generate_ascii_tree,
)
from akasa.state import (
    _JSON_CACHE,
    _JSON_MTIME,
    _JSON_PARSED_CACHE,
    _JSON_CACHE_LOCK,
    heal_daemon_config_files,
    get_system_index_path,
    get_daemon_host_config_path,
    get_json_cache_stats,
    get_kernel_memory_telemetry,
    load_json_config,
    save_json_config,
    get_workspace_physics,
    get_physics_for,
    get_tenant_control_dir,
    get_all_workspace_ids,
    load_config,
    register_core_module,
    get_core_modules,
    is_core_module,
    get_active_modules,
    is_extension_enabled,
    resolve_sandbox_path,
    resolve_system_artifact_path,
)
_cwd = _akasa_context._cwd


def __getattr__(name: str):
    if name in ("_cwd", "_in_test_sandbox"):
        return getattr(_akasa_context, name)
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
