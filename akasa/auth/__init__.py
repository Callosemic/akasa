import os
import json
import socket
import secrets
from flask import Blueprint, request, jsonify
from akasa.utils import load_config, save_json_config, get_workspace_physics
from akasa.extension import AkasaExtension
auth_bp = Blueprint('auth', __name__)
def get_master_key():
    import os
    import shutil
    from akasa.utils import _cwd
    from pathlib import Path

    control_dir_name = os.environ.get("AKASA_CONTROL_DIR", ".akasa")
    global_key_path = Path.home().joinpath(control_dir_name, "master_key.txt")
    local_key_path = Path(_cwd).joinpath(control_dir_name, "master_key.txt")

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
    # Standard Global Key Generation/Loading
    if not global_key_path.exists() or os.path.getsize(global_key_path) == 0:
        from cryptography.fernet import Fernet
        from akasa.vfs import execute_vfs_save
        key = Fernet.generate_key()
        execute_vfs_save("default", global_key_path.as_posix(), key.decode('utf-8'), data={"is_absolute_artifact": True, "ignore_ledger": True})
        return key

    return global_key_path.read_bytes()

def encrypt_secret(val: str) -> str:
    if not val: return val
    from cryptography.fernet import Fernet
    f = Fernet(get_master_key())
    return "v1:" + f.encrypt(val.encode('utf-8')).decode('utf-8')

def decrypt_secret(val: str) -> str:
    if not val or not val.startswith("v1:"):
        return val
    from cryptography.fernet import Fernet
    f = Fernet(get_master_key())
    try:
        return f.decrypt(val[3:].encode('utf-8')).decode('utf-8')
    except Exception:
        return ""
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
@security_bp.route('settings', methods=['POST'])
def update_security_settings(ctx):
    data = ctx.req.json or {}
    new_key = data.get("master_fernet_key")
    if new_key:
        import os
        from pathlib import Path
        from akasa.utils import _cwd
        control_dir_name = os.environ.get("AKASA_CONTROL_DIR", ".akasa")
        global_key_path = Path.home().joinpath(control_dir_name, "master_key.txt")
        local_key_path = Path(_cwd).joinpath(control_dir_name, "master_key.txt")
        # If a local legacy key exists, update that one to avoid breaking state, otherwise update global
        target_path = local_key_path if local_key_path.exists() else global_key_path
        from akasa.vfs import execute_vfs_save
        execute_vfs_save(ctx.workspace_id, target_path.as_posix(), new_key, data={"is_absolute_artifact": True, "ignore_ledger": True})

    return {"status": "success", "requires_refresh": False}
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

        import urllib.parse
        def is_allowed(url_str):
            try:
                hostname = urllib.parse.urlparse(url_str).hostname or ""
                return hostname in ['127.0.0.1', 'localhost'] or hostname.endswith('.ts.net')
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
    from akasa.hooks import hooks
    for res in hooks.emit('execute_identity_handshake', request=request, client_ip=client_ip, config=cfg):
        if res and isinstance(res, dict) and res.get("status") == "authenticated":
            return jsonify({
                "status": "authenticated", 
                "token": BOOT_TOKEN, 
                "method": res.get("method", "hook"),
                "user": res.get("user")
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