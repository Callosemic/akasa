from pathlib import Path
import inspect
import json
import os
import threading
import typing
from akasa.context import (
    get_control_dir_name,
    get_daemon_cwd,
    sniff_tenant_id,
    atomic_write_text,
    clone_json_value,
    thread_safe_cache,
    is_subpath,
)
from akasa.uri import AkasaURI
from akasa.hooks import hooks
from akasa.db import set_db_physics_resolvers
from akasa.vfs import (
    read_cached_physical_text,
    execute_vfs_save,
    get_vfs_cache_stats,
    set_vfs_artifact_path_resolver,
)
_HEAL_LOCAL = threading.local()
_JSON_CACHE_LOCK = threading.RLock()
_JSON_CACHE: typing.Dict[str, str] = {}
_JSON_MTIME: typing.Dict[str, int] = {}
_JSON_PARSED_CACHE: typing.Dict[str, typing.Tuple[int, typing.Any]] = {}
_CORE_MODULES: typing.Set[str] = set()


def heal_daemon_config_files(remove_legacy_system_json: bool = False) -> typing.Tuple[str, str]:
    """
    Kernel-level daemon config healer:
    Synchronizes any legacy system.json into workspaces.json (multi-tenant routing index)
    and host.json (daemon-scoped settings). Optionally removes system.json once split.
    Returns (workspaces_index_path, host_config_path).
    """
    control_dir_name = get_control_dir_name()
    base_dir = Path(get_daemon_cwd()).joinpath(control_dir_name)
    ws_path = base_dir.joinpath("workspaces.json")
    host_path = base_dir.joinpath("host.json")
    sys_path = base_dir.joinpath("system.json")

    if getattr(_HEAL_LOCAL, "active", False):
        return ws_path.as_posix(), host_path.as_posix()

    _HEAL_LOCAL.active = True
    try:
        if sys_path.exists():
            try:
                sys_data = load_json_config(sys_path.as_posix(), {})
                if isinstance(sys_data, dict):
                    sys_ws = sys_data.get("workspaces")
                    existing_ws_data = load_json_config(ws_path.as_posix(), {}) if ws_path.exists() else {}
                    cur_ws = (
                        existing_ws_data.get("workspaces")
                        if isinstance(existing_ws_data, dict) and not existing_ws_data.get("_deprecated")
                        else {}
                    )
                    if not isinstance(cur_ws, dict):
                        cur_ws = {}

                    if isinstance(sys_ws, dict) and (
                        not ws_path.exists()
                        or existing_ws_data.get("_deprecated")
                        or any(k not in cur_ws for k in sys_ws)
                    ):
                        merged_ws = {"default": {"config_path": "config.json"}, **cur_ws, **sys_ws}
                        active_ws = (
                            sys_data.get("active_workspace")
                            or (existing_ws_data.get("active_workspace") if isinstance(existing_ws_data, dict) else None)
                            or "default"
                        )
                        restored = {"workspaces": merged_ws, "active_workspace": active_ws}
                        atomic_write_text(ws_path.as_posix(), json.dumps(restored, indent=2), encoding="utf-8", lock=True)
                        with _JSON_CACHE_LOCK:
                            _JSON_CACHE.pop(ws_path.as_posix(), None)
                            _JSON_MTIME.pop(ws_path.as_posix(), None)
                            _JSON_PARSED_CACHE.pop(ws_path.as_posix(), None)

                    host_keys = {k: v for k, v in sys_data.items() if k not in ("workspaces", "active_workspace")}
                    if host_keys and (remove_legacy_system_json or not host_path.exists()):
                        host_data = load_json_config(host_path.as_posix(), {}) if host_path.exists() else {}
                        merged_host = {**host_keys, **host_data} if host_path.exists() else host_keys
                        atomic_write_text(host_path.as_posix(), json.dumps(merged_host, indent=2), encoding="utf-8", lock=True)
                        with _JSON_CACHE_LOCK:
                            _JSON_CACHE.pop(host_path.as_posix(), None)
                            _JSON_MTIME.pop(host_path.as_posix(), None)
                            _JSON_PARSED_CACHE.pop(host_path.as_posix(), None)

                    if remove_legacy_system_json:
                        try:
                            os.remove(sys_path.as_posix())
                        except OSError:
                            pass
                        with _JSON_CACHE_LOCK:
                            _JSON_CACHE.pop(sys_path.as_posix(), None)
                            _JSON_MTIME.pop(sys_path.as_posix(), None)
                            _JSON_PARSED_CACHE.pop(sys_path.as_posix(), None)
            except Exception:
                pass
        elif ws_path.exists():
            try:
                existing_ws_data = load_json_config(ws_path.as_posix(), {})
                if isinstance(existing_ws_data, dict) and existing_ws_data.get("_deprecated"):
                    restored_default = {
                        "workspaces": {"default": {"config_path": "config.json"}},
                        "active_workspace": "default",
                    }
                    atomic_write_text(ws_path.as_posix(), json.dumps(restored_default, indent=2), encoding="utf-8", lock=True)
                    with _JSON_CACHE_LOCK:
                        _JSON_CACHE.pop(ws_path.as_posix(), None)
                        _JSON_MTIME.pop(ws_path.as_posix(), None)
                        _JSON_PARSED_CACHE.pop(ws_path.as_posix(), None)
            except Exception:
                pass
    finally:
        _HEAL_LOCAL.active = False

    return ws_path.as_posix(), host_path.as_posix()


def get_system_index_path() -> str:
    """Returns the canonical path to the daemon's workspaces.json routing index."""
    ws_path, _ = heal_daemon_config_files(remove_legacy_system_json=True)
    return ws_path


def get_daemon_host_config_path() -> str:
    """Returns the canonical path to the daemon's host.json settings file."""
    _, host_path = heal_daemon_config_files(remove_legacy_system_json=True)
    return host_path


def get_json_cache_stats() -> dict:
    """Returns telemetry statistics for the internal JSON configuration cache."""
    with _JSON_CACHE_LOCK:
        json_cache_bytes = sum(len(v.encode('utf-8')) for v in _JSON_CACHE.values() if isinstance(v, str))
        files_count = len(_JSON_CACHE)
    return {
        "json_config_cache_files": files_count,
        "json_config_cache_kb": round(json_cache_bytes / 1024, 2),
    }


def get_kernel_memory_telemetry() -> dict:
    """Combines VFS read/write cache statistics and JSON config cache statistics into a single telemetry dict."""
    return {
        **get_vfs_cache_stats(),
        **get_json_cache_stats(),
    }


def load_json_config(filepath, default_fallback=None):
    """
    Reads and parses a JSON configuration file backed by the VFS Read-Your-Writes cache.
    Caches parsed Python objects keyed by the VFS causal revision token (-pending_seq or st_mtime_ns),
    returning a fast container clone on cache hits without re-running json.loads().
    """
    if default_fallback is None:
        default_fallback = {}
    if not filepath:
        return clone_json_value(default_fallback)

    norm_key = Path(filepath).as_posix()
    with _JSON_CACHE_LOCK:
        mtime_known = norm_key in _JSON_MTIME and norm_key in _JSON_CACHE

    rev_token, raw_text = read_cached_physical_text(norm_key, bypass_read_cache=not mtime_known)
    if rev_token is None or raw_text is None:
        with _JSON_CACHE_LOCK:
            _JSON_CACHE.pop(norm_key, None)
            _JSON_MTIME.pop(norm_key, None)
            _JSON_PARSED_CACHE.pop(norm_key, None)
        return clone_json_value(default_fallback)

    with _JSON_CACHE_LOCK:
        cached_entry = _JSON_PARSED_CACHE.get(norm_key)
        if mtime_known and cached_entry is not None and cached_entry[0] == rev_token and _JSON_MTIME.get(norm_key) == rev_token:
            return clone_json_value(cached_entry[1])

    try:
        parsed = json.loads(raw_text)
    except Exception:
        return clone_json_value(default_fallback)

    with _JSON_CACHE_LOCK:
        _JSON_CACHE[norm_key] = raw_text
        _JSON_MTIME[norm_key] = rev_token
        _JSON_PARSED_CACHE[norm_key] = (rev_token, parsed)

    return clone_json_value(parsed)


def save_json_config(filepath, data, workspace_id=None):
    """Stages a JSON configuration write onto the VFS pipeline and updates the parsed object cache atomically."""
    norm_key = Path(filepath).as_posix()
    content = json.dumps(data, indent=2)
    cloned_data = clone_json_value(data)

    execute_vfs_save(
        workspace_id or "default",
        norm_key,
        content,
        data={"is_absolute_artifact": True, "ignore_ledger": True},
    )

    rev_token, _ = read_cached_physical_text(norm_key)
    with _JSON_CACHE_LOCK:
        _JSON_CACHE[norm_key] = content
        if rev_token is not None:
            _JSON_MTIME[norm_key] = rev_token
            _JSON_PARSED_CACHE[norm_key] = (rev_token, cloned_data)


def get_workspace_physics(workspace_id=None):
    """Resolves the canonical (config_path, workspace_root) 2-tuple for a tenant workspace."""
    if not workspace_id:
        workspace_id = sniff_tenant_id()
    cwd = get_daemon_cwd()
    control_dir_name = get_control_dir_name()
    local_control_dir = Path(cwd).joinpath(control_dir_name).as_posix()
    default_config = Path(local_control_dir).joinpath("config.json").as_posix()
    index_path = get_system_index_path()
    env_config = os.environ.get("AKASA_CONFIG")
    if env_config:
        resolved_cfg = os.path.abspath(os.path.expanduser(env_config))
        return resolved_cfg, cwd
    target_ws = workspace_id or "default"

    resolved_cfg = default_config
    cfg_path = None
    if os.path.exists(index_path) or read_cached_physical_text(index_path)[0] is not None:
        try:
            w_data = load_json_config(index_path, {})
            registered = w_data.get("workspaces", {})
            if target_ws in registered:
                cfg_path = registered[target_ws].get("config_path")
            elif target_ws != "default":
                raise KeyError(f"Workspace '{target_ws}' is not registered.")
        except KeyError:
            raise
        except Exception:
            pass
    if cfg_path:
        if not os.path.isabs(os.path.expanduser(cfg_path)):
            resolved_cfg = os.path.abspath(Path(local_control_dir).joinpath(cfg_path).as_posix())
        else:
            resolved_cfg = os.path.abspath(os.path.expanduser(cfg_path))
    if not os.path.exists(resolved_cfg) and target_ws == "default":
        resolved_cfg = default_config

    if os.path.exists(resolved_cfg) or read_cached_physical_text(resolved_cfg)[0] is not None:
        try:
            c_data = load_json_config(resolved_cfg, {})
            if "workspace_root" in c_data:
                return resolved_cfg, Path(c_data["workspace_root"]).expanduser().resolve().as_posix()
        except Exception:
            pass

        return resolved_cfg, Path(resolved_cfg).parent.parent.as_posix()

    if target_ws != "default":
        return resolved_cfg, Path(resolved_cfg).parent.parent.as_posix()

    return resolved_cfg, cwd


def get_physics_for(workspace_id):
    """Alias for get_workspace_physics(workspace_id)."""
    return get_workspace_physics(workspace_id)


def get_tenant_control_dir(workspace_id=None):
    """Returns the control directory path for the specified tenant workspace."""
    cfg_path, _ = get_workspace_physics(workspace_id)
    return Path(cfg_path).parent.as_posix()


def get_all_workspace_ids():
    """Returns a list of all registered tenant workspace IDs (always including 'default')."""
    index_path = get_system_index_path()
    workspace_ids = ["default"]
    if os.path.exists(index_path) or read_cached_physical_text(index_path)[0] is not None:
        try:
            w_data = load_json_config(index_path, {})
            workspace_ids = list(w_data.get("workspaces", {}).keys())
            if "default" not in workspace_ids:
                workspace_ids.append("default")
        except Exception:
            pass
    return workspace_ids


@thread_safe_cache(ttl=2.0, key_maker=lambda workspace_id=None, **kwargs: str(workspace_id))
def load_config(workspace_id=None):
    """Loads the active tenant's config.json and applies mutate_workspace_config hook listeners."""
    cfg_path, _ = get_workspace_physics(workspace_id)
    cfg = load_json_config(cfg_path, {})
    hooks.emit('mutate_workspace_config', cfg, workspace_id=workspace_id)
    return cfg


@hooks.on('config_mutated')
def _invalidate_config_on_mutation(workspace_id=None, **kwargs):
    if workspace_id:
        load_config.invalidate(workspace_id=workspace_id)
    else:
        load_config.clear_all()


def register_core_module(ext_name):
    """DI Interface: Host OS declares its foundational modules."""
    _CORE_MODULES.add(ext_name)


def get_core_modules() -> set:
    """Returns a copy of the registered foundational core module names."""
    return set(_CORE_MODULES)


def is_core_module(ext_name):
    """Returns True if ext_name is registered as a foundational core module."""
    return ext_name in _CORE_MODULES


def get_active_modules(workspace_id=None) -> list:
    """Returns a sorted list of all active modules (registered core modules + enabled workspace extensions)."""
    cfg = load_config(workspace_id)
    return sorted(set(_CORE_MODULES) | set(cfg.get("extensions", [])))


def is_extension_enabled(ext_name, workspace_id=None):
    """Returns True if ext_name is a core module or enabled in the active workspace's extensions list."""
    if is_core_module(ext_name):
        return True
    cfg = load_config(workspace_id)
    return ext_name in cfg.get("extensions", [])


def _authorize_hook_callback(cb, event_name: str, workspace_id: typing.Optional[str], trusted_namespaces: set) -> bool:
    """Ring 2 Hook Authorization Evaluator injected into Ring 1 HookRegistry."""
    target_cb = inspect.unwrap(cb)
    mod = getattr(target_cb, '__module__', None) or cb.__module__ or ""

    stamped_ext = getattr(cb, '_akasa_ext_name', None) or getattr(target_cb, '_akasa_ext_name', None)
    is_core_flag = getattr(cb, 'is_core', False) or getattr(target_cb, 'is_core', False)
    if is_core_flag or (stamped_ext and is_core_module(stamped_ext)):
        return True
    if stamped_ext:
        if event_name == 'system_boot':
            print(f"⚠️ [Event Bus] Banned hook: Non-core extension '{stamped_ext}' attempted to subscribe to 'system_boot'. Use 'workspace_boot' instead.")
            return False
        return is_extension_enabled(stamped_ext, workspace_id)

    if not mod or mod.startswith('akasa.'):
        return True

    if any(mod.startswith(ns) for ns in trusted_namespaces):
        return True

    ext_name = None
    try:
        enabled_exts = set(load_config(workspace_id).get("extensions", []))
        for part in mod.split('.'):
            if part in enabled_exts:
                ext_name = part
                break
    except Exception:
        pass

    if event_name == 'system_boot':
        if ext_name and is_core_module(ext_name):
            return True
        if any(is_core_module(p) for p in mod.split('.')):
            return True
        print(f"⚠️ [Event Bus] Banned hook: Non-core extension '{mod}' attempted to subscribe to 'system_boot'. Use 'workspace_boot' instead.")
        return False

    if ext_name:
        return is_extension_enabled(ext_name, workspace_id)

    return True


def resolve_sandbox_path(filepath, workspace_id=None):
    """Tier 1 Safe Path Resolution: Prevents directory traversal breakouts."""
    _, workspace_root = get_workspace_physics(workspace_id)
    ws_root_path = Path(workspace_root).resolve()

    filepath_str = str(filepath)
    uri = AkasaURI(filepath_str)
    norm_path = uri.path

    if Path(norm_path).is_absolute():
        resolved_abs = Path(norm_path).resolve()
        if is_subpath(resolved_abs, ws_root_path):
            return resolved_abs.as_posix()
        try:
            norm_path = resolved_abs.relative_to(ws_root_path).as_posix()
        except ValueError:
            norm_path = resolved_abs.name

    resolved = ws_root_path.joinpath(norm_path).resolve()
    if not is_subpath(resolved, ws_root_path):
        return ws_root_path.as_posix()
    return resolved.as_posix()


def resolve_system_artifact_path(filepath, workspace_id):
    """Safely resolves a system artifact path inside active or registered tenant control directories."""
    cfg_path, _ = get_workspace_physics(workspace_id)
    control_dir_name = get_control_dir_name()

    active_control_base = Path(cfg_path).parent.resolve()
    default_control_base = Path(get_daemon_cwd()).joinpath(control_dir_name).resolve()
    resolved_abs = Path(filepath).resolve()

    if is_subpath(resolved_abs, active_control_base) or is_subpath(resolved_abs, default_control_base):
        return resolved_abs.as_posix()

    for ws_id in get_all_workspace_ids():
        try:
            ws_control = Path(get_tenant_control_dir(ws_id)).resolve()
            if is_subpath(resolved_abs, ws_control):
                return resolved_abs.as_posix()
        except Exception:
            pass

    return resolve_sandbox_path(filepath, workspace_id)
def reset() -> None:
    """Purges JSON configuration caches and load_config TTL cache for isolated test harnesses."""
    with _JSON_CACHE_LOCK:
        _JSON_CACHE.clear()
        _JSON_MTIME.clear()
        _JSON_PARSED_CACHE.clear()
    load_config.clear_all()


# Wire Ring 2 state callbacks into Ring 1 (HookRegistry) and Ring 2 peers (db, vfs)
hooks.set_hook_authorizer(_authorize_hook_callback)
set_db_physics_resolvers(get_workspace_physics, get_all_workspace_ids)
set_vfs_artifact_path_resolver(resolve_system_artifact_path)