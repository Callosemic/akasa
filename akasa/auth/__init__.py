import os
import json
import shutil
import socket
import secrets
import urllib.parse
from pathlib import Path
from cryptography.fernet import Fernet
from flask import Blueprint, request, jsonify
import akasa.utils as _akasa_utils
from akasa.utils import (
    load_config,
    load_json_config,
    save_json_config,
    get_workspace_physics,
    get_all_workspace_ids,
    get_tenant_control_dir,
    get_trusted_host_suffixes,
)
from akasa.extension import AkasaExtension
from akasa.hooks import hooks

auth_bp = Blueprint('auth', __name__)

def get_master_key():
    control_dir_name = os.environ.get("AKASA_CONTROL_DIR", ".akasa")
    global_key_path = Path.home().joinpath(control_dir_name, "master_key.txt")
    local_key_path = Path(_akasa_utils._cwd).joinpath(control_dir_name, "master_key.txt")

    # Safe Migration: If a legacy local key exists...
    if local_key_path.exists() and os.path.getsize(local_key_path) > 0:
        # If no global key exists yet, elevate this local key to be the global key
        if not global_key_path.exists():
            global_key_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(local_key_path.as_posix(), global_key_path.as_posix())
            return global_key_path.read_bytes()
        else:
            # A global key already exists. This local key is different.
            # We MUST keep using the local one here so we don't brick this specific workspace's secrets.
            return local_key_path.read_bytes()
    # Standard Global Key Generation/Loading (Synchronous write prevents first-boot race conditions)
    if not global_key_path.exists() or os.path.getsize(global_key_path) == 0:
        key = Fernet.generate_key()
        global_key_path.parent.mkdir(parents=True, exist_ok=True)
        global_key_path.write_bytes(key)
        try:
            os.chmod(global_key_path.as_posix(), 0o600)
        except OSError:
            pass
        return key

    return global_key_path.read_bytes()

def ensure_master_key_on_disk() -> bytes:
    """Synchronously guarantees the Fernet master key exists on disk."""
    return get_master_key()

def encrypt_secret(val: str) -> str:
    if not val: return val
    f = Fernet(get_master_key())
    return "v1:" + f.encrypt(val.encode('utf-8')).decode('utf-8')

def decrypt_secret(val: str) -> str:
    if not val or not val.startswith("v1:"):
        return val
    f = Fernet(get_master_key())
    try:
        return f.decrypt(val[3:].encode('utf-8')).decode('utf-8')
    except Exception:
        return ""
def rotate_master_key(new_key_str: str) -> bool:
    """Validates a new Fernet key, re-encrypts all workspace secrets.json files, and updates master_key.txt."""
    if not new_key_str or not isinstance(new_key_str, str):
        return False
    clean_key = new_key_str.strip()
    old_key_bytes = get_master_key().strip()
    if clean_key.encode('utf-8') == old_key_bytes:
        return True

    new_fernet = Fernet(clean_key.encode('utf-8'))
    old_fernet = Fernet(old_key_bytes)

    def _reencrypt_obj(obj):
        if isinstance(obj, dict):
            return {k: _reencrypt_obj(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [_reencrypt_obj(v) for v in obj]
        elif isinstance(obj, str) and obj.startswith("v1:"):
            try:
                plain = old_fernet.decrypt(obj[3:].encode('utf-8')).decode('utf-8')
                return "v1:" + new_fernet.encrypt(plain.encode('utf-8')).decode('utf-8')
            except Exception:
                return obj
        return obj

    for ws_id in get_all_workspace_ids():
        try:
            ctrl_dir = get_tenant_control_dir(ws_id)
            secrets_path = Path(ctrl_dir).joinpath("secrets.json").as_posix()
            if os.path.exists(secrets_path):
                s_data = load_json_config(secrets_path, {})
                if s_data:
                    rotated_data = _reencrypt_obj(s_data)
                    save_json_config(secrets_path, rotated_data, workspace_id=ws_id)
        except Exception as e:
            print(f"⚠️ [Security] Failed to re-encrypt secrets for workspace '{ws_id}': {e}")
    control_dir_name = os.environ.get("AKASA_CONTROL_DIR", ".akasa")
    global_key_path = Path.home().joinpath(control_dir_name, "master_key.txt")
    local_key_path = Path(_akasa_utils._cwd).joinpath(control_dir_name, "master_key.txt")
    target_path = local_key_path if local_key_path.exists() else global_key_path
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_bytes(clean_key.encode('utf-8'))
    try:
        os.chmod(target_path.as_posix(), 0o600)
    except OSError:
        pass
    return True


security_bp = AkasaExtension(
    'security', __name__, title="Security & Encryption", 
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
@hooks.on('security_settings_updated')
def _on_security_settings_updated(workspace_id=None, **kwargs):
    control_dir_name = os.environ.get("AKASA_CONTROL_DIR", ".akasa")
    sys_path = Path(_akasa_utils._cwd).joinpath(control_dir_name, "system.json").as_posix()
    sys_data = load_json_config(sys_path, {})
    sec_block = sys_data.get("security", {})
    new_key = sec_block.pop("master_fernet_key", None) if isinstance(sec_block, dict) else None
    if new_key is not None:
        if not sec_block:
            sys_data.pop("security", None)
        save_json_config(sys_path, sys_data, workspace_id="default")
        try:
            rotate_master_key(new_key)
        except Exception as e:
            print(f"⚠️ [Security] Master key rotation rejected (invalid Fernet key): {e}")
    return {"requires_refresh": False}
# Generate a cryptographically sound scrolling runtime session token
# Anchor to the environment so it survives os.execv() and Werkzeug hot-reloads
BOOT_TOKEN = os.environ.get("AKASA_BOOT_TOKEN")
if not BOOT_TOKEN:
    BOOT_TOKEN = secrets.token_hex(16)
    os.environ["AKASA_BOOT_TOKEN"] = BOOT_TOKEN
@auth_bp.route('/auth/bootstrap', methods=['POST'])
def bootstrap():
    """Unauthenticated token exchange gate supporting absolute dynamic fallback routing."""
    data = request.json or {}

    # Extract real IP if behind Tailscale Serve or a Reverse Proxy
    client_ip = request.headers.get('X-Forwarded-For', request.remote_addr).split(',')[0].strip()
    # --- ROUTE A: LOCALHOST BYPASS ---
    # If the user is physically on the machine, auto-authenticate
    if client_ip == '127.0.0.1':
        # Check for cross-origin browser requests (Localhost Drive-By CSRF prevention)
        origin = request.headers.get('Origin', '')
        referer = request.headers.get('Referer', '')
        def is_allowed(url_str):
            try:
                hostname = urllib.parse.urlparse(url_str).hostname or ""
                if hostname in ['127.0.0.1', 'localhost']:
                    return True
                trusted_suffixes = get_trusted_host_suffixes()
                if trusted_suffixes and any(hostname.endswith(sfx) for sfx in trusted_suffixes):
                    return True
                return bool(hooks.emit_first('is_trusted_origin', url=url_str, hostname=hostname, predicate=bool))
            except Exception:
                return False

        if origin and not is_allowed(origin):
            return jsonify({"error": "Forbidden: Cross-origin request blocked"}), 403
        if referer and not is_allowed(referer):
            return jsonify({"error": "Forbidden: Cross-origin request blocked"}), 403

        return jsonify({
            "status": "authenticated", 
            "token": BOOT_TOKEN, 
            "method": "localhost"
        })
    cfg = load_config()
    # --- ROUTE B: INVERSION OF CONTROL (Zero-Knowledge Identity Protocols) ---
    auth_res = hooks.emit_first(
        'execute_identity_handshake',
        request=request,
        client_ip=client_ip,
        config=cfg,
        predicate=lambda r: isinstance(r, dict) and r.get("status") == "authenticated"
    )
    if auth_res:
        return jsonify({
            "status": "authenticated", 
            "token": BOOT_TOKEN, 
            "method": auth_res.get("method", "hook"),
            "user": auth_res.get("user")
        })

    # --- ROUTE C: PERSISTENT CONFIG TOKEN CHECK ---
    client_token = data.get("token")
    system_token = cfg.get("auth_token")

    if system_token and client_token == system_token:
        return jsonify({
            "status": "authenticated", 
            "token": BOOT_TOKEN, 
            "method": "static_config"
        })

    return jsonify({
        "status": "challenge", 
        "message": "Authentication required."
    }), 401