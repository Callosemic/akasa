from pathlib import Path
from functools import wraps
import os
import gc
import re
import copy
import json
import time
import ctypes
import typing
import functools
import threading
import posixpath
import unicodedata
import urllib.parse
from akasa.uri import AkasaURI

_cwd = os.getcwd()
def reclaim_process_memory():
    """Tier 1 Kernel Memory Governance: Forces Python garbage collection and releases free glibc arenas back to the OS."""
    gc.collect()
    try:
        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except Exception:
        pass


def sniff_tenant_id():
    try:
        from flask import request as _request
        if _request and 'X-Workspace-ID' in _request.headers:
            return _request.headers['X-Workspace-ID']
    except RuntimeError:
        pass
    return "default"

def get_physics_for(workspace_id):
    return get_workspace_physics(workspace_id)
def get_tenant_control_dir(workspace_id=None):
    cfg_path, _ = get_workspace_physics(workspace_id)
    return Path(cfg_path).parent.as_posix()
def get_workspace_physics(workspace_id=None):
    if not workspace_id:
        workspace_id = sniff_tenant_id()
    control_dir_name = os.environ.get("AKASA_CONTROL_DIR", ".akasa")
    local_control_dir = Path(_cwd).joinpath(control_dir_name).as_posix()
    default_config = Path(local_control_dir).joinpath("config.json").as_posix()
    index_path = Path(local_control_dir).joinpath("system.json").as_posix()
    env_config = os.environ.get("AKASA_CONFIG")
    if env_config:
        resolved_cfg = os.path.abspath(os.path.expanduser(env_config))
        return resolved_cfg, _cwd
    target_ws = workspace_id or "default"

    resolved_cfg = default_config
    cfg_path = None
    if os.path.exists(index_path):
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

    if os.path.exists(resolved_cfg):
        try:
            c_data = load_json_config(resolved_cfg, {})
            if "workspace_root" in c_data:
                return resolved_cfg, Path(c_data["workspace_root"]).expanduser().resolve().as_posix()
        except Exception:
            pass

        return resolved_cfg, Path(resolved_cfg).parent.parent.as_posix()

    if target_ws != "default":
        return resolved_cfg, Path(resolved_cfg).parent.parent.as_posix()

    return resolved_cfg, _cwd
_CORE_MODULES = set()

def register_core_module(ext_name):
    """DI Interface: Host OS declares its foundational modules."""
    _CORE_MODULES.add(ext_name)

def is_core_module(ext_name):
    return ext_name in _CORE_MODULES

def is_extension_enabled(ext_name, workspace_id=None):
    if is_core_module(ext_name):
        return True
    cfg = load_config(workspace_id)
    return ext_name in cfg.get("extensions", [])
def extension_auth(ext_name):
    def decorator(f):
        @functools.wraps(f)
        def decorated_function(*args, **kwargs):
            from flask import jsonify as _jsonify
            ws_id = kwargs.get('workspace_id') or sniff_tenant_id()
            if not is_extension_enabled(ext_name, ws_id):
                return _jsonify({"error": f"403 Forbidden: Extension '{ext_name}' is not enabled in this workspace."}), 403
            return f(*args, **kwargs)
        return decorated_function
    return decorator

def thread_safe_cache(ttl=2.0, key_maker=lambda *args, **kwargs: "global"):
    """
    A non-blocking, single-flight TTL cache with invalidation race protection.
    Prevents lock contention during invalidation and eliminates DB cache stampedes.
    """
    def decorator(func):
        cache = {}
        compilation_locks = {}
        invalidated_at = {}
        state_lock = threading.Lock()

        def _get_key_lock(key):
            with state_lock:
                if key not in compilation_locks:
                    compilation_locks[key] = threading.Lock()
                return compilation_locks[key]
        @wraps(func)
        def wrapper(*args, **kwargs):
            key = key_maker(*args, **kwargs)
            now = time.time()
            # 1. Fast Lock-Free Read
            has_cached = False
            cached_val = None
            with state_lock:
                if key in cache and (not ttl or now - cache[key]['ts'] < ttl):
                    has_cached = True
                    cached_val = cache[key]['val']
            if has_cached:
                return dict(cached_val) if isinstance(cached_val, dict) else (list(cached_val) if isinstance(cached_val, list) else cached_val)

            # 2. Single-Flight Execution Barrier
            key_lock = _get_key_lock(key)
            with key_lock:
                # Re-check cache in case another thread computed it while waiting
                has_cached = False
                cached_val = None
                with state_lock:
                    if key in cache and (not ttl or now - cache[key]['ts'] < ttl):
                        has_cached = True
                        cached_val = cache[key]['val']

                if has_cached:
                    return dict(cached_val) if isinstance(cached_val, dict) else (list(cached_val) if isinstance(cached_val, list) else cached_val)

                calc_start = time.time()
                result = func(*args, **kwargs)

                with state_lock:
                    # Discard result if an invalidation occurred during calculation
                    if invalidated_at.get(key, 0) < calc_start:
                        cache[key] = {'ts': time.time(), 'val': result}
                    return dict(result) if isinstance(result, dict) else (list(result) if isinstance(result, list) else result)

        def invalidate(*args, **kwargs):
            key = key_maker(*args, **kwargs)
            with state_lock:
                invalidated_at[key] = time.time()
                if key in cache:
                    del cache[key]

        def clear_all():
            with state_lock:
                now = time.time()
                for k in list(cache.keys()):
                    invalidated_at[k] = now
                cache.clear()

        wrapper.invalidate = invalidate
        wrapper.clear_all = clear_all
        return wrapper
    return decorator
_JSON_CACHE = {}
_JSON_MTIME = {}
def load_json_config(filepath, default_fallback=None):
    global _JSON_CACHE, _JSON_MTIME
    if default_fallback is None:
        default_fallback = {}

    if not filepath:
        return default_fallback

    try:
        current_mtime = os.stat(filepath).st_mtime_ns
    except OSError:
        return default_fallback
    if filepath not in _JSON_CACHE or current_mtime > _JSON_MTIME.get(filepath, 0):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                raw_text = f.read()
                json.loads(raw_text)
                _JSON_CACHE[filepath] = raw_text
        except Exception:
            _JSON_CACHE[filepath] = json.dumps(default_fallback)
        _JSON_MTIME[filepath] = current_mtime

    return json.loads(_JSON_CACHE[filepath])
def save_json_config(filepath, data, workspace_id=None):
    global _JSON_CACHE, _JSON_MTIME
    from akasa.vfs import execute_vfs_save as _execute_vfs_save

    content = json.dumps(data, indent=2)
    _execute_vfs_save(workspace_id or "default", filepath, content, data={"is_absolute_artifact": True, "ignore_ledger": True})

    _JSON_CACHE[filepath] = content
    _JSON_MTIME[filepath] = time.time_ns()
_is_invalidator_bound = False
@thread_safe_cache(ttl=2.0, key_maker=lambda workspace_id=None, **kwargs: str(workspace_id))
def load_config(workspace_id=None):
    global _is_invalidator_bound
    from akasa.hooks import hooks as _hooks

    # Decoupled Invalidation: Bind the cache invalidator on first execution to prevent circular imports
    if not _is_invalidator_bound:
        @_hooks.on('config_mutated')
        def _invalidate_config(workspace_id=None, **kwargs):
            if workspace_id:
                load_config.invalidate(workspace_id=workspace_id)
            else:
                load_config.clear_all()
        _is_invalidator_bound = True

    cfg_path, _ = get_workspace_physics(workspace_id)
    cfg = load_json_config(cfg_path, {})

    _hooks.emit('mutate_workspace_config', cfg, workspace_id=workspace_id)

    return cfg
def get_all_workspace_ids():
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
    return workspace_ids
def typeddict_to_openapi(td_class):
    """Transforms a Python TypedDict into an OpenAPI 3.0 JSON Schema object natively."""
    if not hasattr(td_class, "__annotations__"):
        return {"type": "object"}

    properties = {}
    required = []

    # Support Python 3.9+ runtime required keys extraction
    req_keys = getattr(td_class, '__required_keys__', td_class.__annotations__.keys())

    for key, typ in td_class.__annotations__.items():
        if key in req_keys:
            required.append(key)

        origin = typing.get_origin(typ)
        args = typing.get_args(typ)

        # Handle Optional[X] / Union[X, None]
        if origin is typing.Union and type(None) in args:
            typ = next(a for a in args if a is not type(None))
            origin = typing.get_origin(typ)
            args = typing.get_args(typ)
        # Native type mapping
        if origin is typing.Literal:
            properties[key] = {"type": "string", "enum": list(args)}
        elif origin is list or origin is typing.List or typ is list:
            item_type = {"type": "string"} # Default fallback
            if args:
                if hasattr(args[0], '__annotations__'):
                    item_type = typeddict_to_openapi(args[0])
                else:
                    item_str = str(args[0]).lower()
                    if 'int' in item_str: item_type = {"type": "integer"}
                    elif 'float' in item_str: item_type = {"type": "number"}
                    elif 'bool' in item_str: item_type = {"type": "boolean"}
                    elif 'dict' in item_str or 'object' in item_str: item_type = {"type": "object", "additionalProperties": True}
            properties[key] = {"type": "array", "items": item_type}
        elif origin is dict or origin is typing.Dict or typ is dict:
            properties[key] = {"type": "object", "additionalProperties": True}
        elif hasattr(typ, '__annotations__'):
            properties[key] = typeddict_to_openapi(typ)
        else:
            type_str = str(typ).lower()
            if 'str' in type_str: properties[key] = {"type": "string"}
            elif 'int' in type_str: properties[key] = {"type": "integer"}
            elif 'float' in type_str: properties[key] = {"type": "number"}
            elif 'bool' in type_str: properties[key] = {"type": "boolean"}
            elif 'bytes' in type_str or 'file' in type_str: properties[key] = {"type": "string", "format": "binary"}
            else: properties[key] = {"type": "object"}

    schema = {"type": "object", "properties": properties}
    if required:
        schema["required"] = list(required)

    return schema
def generate_idempotency_hash(payload: dict) -> str:
    return json.dumps(payload, sort_keys=True)

def parse_uri(path_str):
    """SDK Helper: Parses any scheme:// or relative path into (volume, relative_path)."""
    if not path_str:
        return "", ""

    uri = AkasaURI(path_str)
    # Strict mapping for fully qualified URIs
    if uri.scheme:
        return uri.volume, uri.path
    # Legacy fallback for scheme-less paths (e.g. 'repo/file.txt')
    clean_path = str(path_str).replace('\\', '/').strip()
    fallback_uri = AkasaURI(f"vfs://{clean_path}")
    if fallback_uri.volume and (fallback_uri.path or '/' in clean_path):
        return fallback_uri.volume, fallback_uri.path
    elif fallback_uri.volume and not fallback_uri.path:
        return fallback_uri.volume, ""
    return "", uri.path
def fold_vfs_events(events):
    """
    Mathematical state machine for VFS event stream folding.
    Reduces a chronological list of high-velocity VFS events down to their net-effective state.
    """
    folded = {}

    for ev in events:
        # Support both dictionaries and future BaseJobPayload dataclasses
        fp = ev.get('filepath') if isinstance(ev, dict) else getattr(ev, 'filepath', None)
        op = ev.get('mutation_type', ev.get('operation', 'save')) if isinstance(ev, dict) else getattr(ev, 'event_type', 'save')

        if not fp:
            continue

        op = op.lower()
        is_dir = fp.endswith('/')

        if op in ('delete', 'deleted', 'remove', 'removed'):
            # Hierarchical Pruning: Annihilate any preceding events for children of this directory
            dir_prefix = fp if is_dir else f"{fp}/"
            keys_to_remove = [k for k in folded.keys() if k.startswith(dir_prefix)]
            for k in keys_to_remove:
                del folded[k]

            # Annihilation: If the file was explicitly CREATED in this window, and now deleted, it never existed.
            # If it was just MODIFIED, the net effect is still a DELETION.
            if fp in folded:
                first_op = folded[fp].get('original_op', '')
                if first_op in ('added', 'create', 'created'):
                    del folded[fp]
                    continue

            # Otherwise, record the net deletion
            folded[fp] = {'op': 'delete', 'event': ev, 'original_op': 'delete'}

        elif op in ('save', 'modified', 'added', 'create', 'created'):
            # Coalescence: Overwrite previous events. Preserve the original operation type if it was a creation.
            original_op = op
            if fp in folded and folded[fp]['op'] not in ('delete', 'deleted'):
                original_op = folded[fp].get('original_op', op)

            folded[fp] = {'op': 'save', 'event': ev, 'original_op': original_op}

    # Extract the surviving net-effective events
    return [state['event'] for state in folded.values()]
def slugify(text):
    if not text:
        return ""
    charmap = {
        'Đ': 'D', 'đ': 'd',
        'Æ': 'AE', 'æ': 'ae',
        'Ø': 'O', 'ø': 'o',
        'ß': 'ss'
    }
    for broken, fixed in charmap.items():
        text = text.replace(broken, fixed)
    clean = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8')
    clean = re.sub(r'[^a-zA-Z0-9_-]', '_', clean.lower())
    return re.sub(r'_+', '_', clean).strip('_')
def is_subpath(child_path, parent_path) -> bool:
    """Returns True if child_path resolves inside or equal to parent_path without prefix collision."""
    try:
        c = Path(child_path).resolve()
        p = Path(parent_path).resolve()
        return c == p or p in c.parents
    except Exception:
        return False


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
    cfg_path, _ = get_workspace_physics(workspace_id)
    control_dir_name = os.environ.get("AKASA_CONTROL_DIR", ".akasa")

    active_control_base = Path(cfg_path).parent.resolve()
    default_control_base = Path(_cwd).joinpath(control_dir_name).resolve()
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
def build_tree_dict(file_paths):
    tree = {}
    for path in file_paths:
        parts = Path(path).parts
        current = tree
        for part in parts:
            if part not in current:
                current[part] = {}
            current = current[part]
    return tree
def generate_ascii_tree(file_paths):
    tree = build_tree_dict(file_paths)
    def print_tree(node, prefix=""):
        lines = []
        entries = sorted(list(node.keys()))
        for i, key in enumerate(entries):
            is_last = (i == len(entries) - 1)
            lines.append(f"{prefix}{'└── ' if is_last else '├── '}{key}")
            lines.extend(print_tree(node[key], prefix + ("    " if is_last else "│   ")))
        return lines
    return ".\n" + "\n".join(print_tree(tree))
