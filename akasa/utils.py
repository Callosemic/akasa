from pathlib import Path
import os
import json

_cwd = os.getcwd()
def sniff_tenant_id():
    try:
        from flask import request
        if request and 'X-Workspace-ID' in request.headers:
            return request.headers['X-Workspace-ID']
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
CORE_MODULES = set()
def _discover_core_modules():
    """JIT Hook evaluation to retrieve host-provided core modules."""
    from akasa.hooks import hooks
    modules = set()
    for res in hooks.emit('register_core_modules'):
        if res: modules.update(res)
    return modules

def is_core_module(ext_name):
    if not CORE_MODULES:
        CORE_MODULES.update(_discover_core_modules())
    return ext_name in CORE_MODULES

def is_extension_enabled(ext_name, workspace_id=None):
    if is_core_module(ext_name):
        return True
    cfg = load_config(workspace_id)
    return ext_name in cfg.get("extensions", [])
def extension_auth(ext_name):
    def decorator(f):
        import functools
        @functools.wraps(f)
        def decorated_function(*args, **kwargs):
            from flask import jsonify
            ws_id = kwargs.get('workspace_id') or sniff_tenant_id()
            if not is_extension_enabled(ext_name, ws_id):
                return jsonify({"error": f"403 Forbidden: Extension '{ext_name}' is not enabled in this workspace."}), 403
            return f(*args, **kwargs)
        return decorated_function
    return decorator

_JSON_CACHE = {}
_JSON_MTIME = {}
def load_json_config(filepath, default_fallback=None):
    global _JSON_CACHE, _JSON_MTIME
    if default_fallback is None:
        default_fallback = {}

    if not filepath or not os.path.exists(filepath):
        return default_fallback

    current_mtime = os.path.getmtime(filepath)
    if filepath not in _JSON_CACHE or current_mtime > _JSON_MTIME.get(filepath, 0):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                _JSON_CACHE[filepath] = json.load(f)
        except Exception:
            _JSON_CACHE[filepath] = default_fallback
        _JSON_MTIME[filepath] = current_mtime

    return _JSON_CACHE[filepath]
def save_json_config(filepath, data, workspace_id=None):
    global _JSON_CACHE, _JSON_MTIME
    import json
    import time
    from akasa.vfs import execute_vfs_save

    content = json.dumps(data, indent=2)
    execute_vfs_save(workspace_id or "default", filepath, content, data={"is_absolute_artifact": True, "ignore_ledger": True})

    _JSON_CACHE[filepath] = data
    _JSON_MTIME[filepath] = time.time()

_MUTATED_CONFIG_CACHE = {}
_MUTATED_CONFIG_MTIME = {}
def load_config(workspace_id=None):
    cfg_path, _ = get_workspace_physics(workspace_id)
    current_mtime = os.path.getmtime(cfg_path) if os.path.exists(cfg_path) else 0
    if cfg_path not in _MUTATED_CONFIG_CACHE or current_mtime > _MUTATED_CONFIG_MTIME.get(cfg_path, 0):
        import copy
        cfg = copy.deepcopy(load_json_config(cfg_path, {}))
        from akasa.hooks import hooks
        hooks.emit('mutate_workspace_config', cfg, workspace_id=workspace_id)
        _MUTATED_CONFIG_CACHE[cfg_path] = cfg
        _MUTATED_CONFIG_MTIME[cfg_path] = current_mtime

    return _MUTATED_CONFIG_CACHE[cfg_path]
def get_all_workspace_ids():
    control_dir_name = os.environ.get("AKASA_CONTROL_DIR", ".akasa")
    index_path = Path(_cwd).joinpath(control_dir_name, "system.json").as_posix()
    workspace_ids = ["default"]
    if os.path.exists(index_path):
        try:
            with open(index_path, 'r', encoding='utf-8') as f:
                w_data = json.load(f)
            workspace_ids = list(w_data.get("workspaces", {}).keys())
            if "default" not in workspace_ids:
                workspace_ids.append("default")
        except Exception:
            pass
    return workspace_ids
def generate_idempotency_hash(payload: dict) -> str:
    return json.dumps(payload, sort_keys=True)
import urllib.parse
import posixpath
import re
def parse_uri(path_str):
    """SDK Helper: Parses any scheme:// or relative path into (volume, relative_path)."""
    if not path_str:
        return "", ""

    from akasa.uri import AkasaURI
    uri = AkasaURI(path_str)
    # Strict mapping for fully qualified URIs
    if uri.scheme:
        return uri.volume, uri.path

    # Legacy fallback for scheme-less paths (e.g. 'repo/file.txt')
    fallback_uri = AkasaURI(f"vfs://{uri.path}")
    if fallback_uri.volume and fallback_uri.path:
        return fallback_uri.volume, fallback_uri.path
    return "", uri.path

def slugify(text):
    if not text:
        return ""
    import unicodedata
    import re
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
def resolve_sandbox_path(filepath, workspace_id=None):
    """Tier 1 Safe Path Resolution: Prevents directory traversal breakouts."""
    _, workspace_root = get_workspace_physics(workspace_id)
    ws_root_path = Path(workspace_root).resolve()

    from akasa.uri import AkasaURI
    # The URI primitive natively strips schemes and squashes traversals mathematically
    uri = AkasaURI(str(filepath))
    norm_path = uri.path

    if Path(norm_path).is_absolute():
        resolved_abs = Path(norm_path).resolve()
        if str(resolved_abs).startswith(str(ws_root_path)):
            return resolved_abs.as_posix()
        try:
            norm_path = resolved_abs.relative_to(ws_root_path).as_posix()
        except ValueError:
            norm_path = resolved_abs.name
    resolved = ws_root_path.joinpath(norm_path).resolve()
    return resolved.as_posix()
def resolve_system_artifact_path(filepath, workspace_id):
    cfg_path, _ = get_workspace_physics(workspace_id)
    control_dir_name = os.environ.get("AKASA_CONTROL_DIR", ".akasa")

    active_control_base = Path(cfg_path).parent.resolve()
    default_control_base = Path(_cwd).joinpath(control_dir_name).resolve()
    resolved_abs = Path(filepath).resolve()

    if str(resolved_abs).startswith(str(active_control_base)) or str(resolved_abs).startswith(str(default_control_base)):
        return resolved_abs.as_posix()
    else:
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
