import os
import queue
import threading
from typing import Optional, Iterable, Tuple
from flask import Flask, request, jsonify, Response
from akasa.hooks import hooks
import akasa.utils as _akasa_utils
from akasa.utils import (
    register_core_module,
    register_trusted_host_suffix,
    get_trusted_host_suffixes,
)


def tune_process_resources() -> None:
    """Tier 1 OS Resource Governance: Raises file descriptor limits, bounds glibc arenas, and caps thread stack size."""
    try:
        import resource as _resource
        soft_lim, hard_lim = _resource.getrlimit(_resource.RLIMIT_NOFILE)
        target_lim = min(hard_lim, 65536) if hard_lim > 0 else 65536
        if soft_lim < target_lim:
            _resource.setrlimit(_resource.RLIMIT_NOFILE, (target_lim, hard_lim))
    except Exception:
        pass

    os.environ.setdefault("MALLOC_ARENA_MAX", "2")
    try:
        threading.stack_size(1024 * 1024)
    except Exception:
        pass


class ForceHTTPSProxyFix:
    """WSGI Middleware to force HTTPS scheme when behind reverse proxies or trusted tunnel suffixes."""
    def __init__(self, wsgi_app):
        self.app = wsgi_app

    def __call__(self, environ, start_response):
        host = environ.get("HTTP_HOST", "")
        trusted_suffixes = get_trusted_host_suffixes()
        if environ.get("HTTP_X_FORWARDED_PROTO") == "https" or (
            trusted_suffixes and any(host.endswith(sfx) for sfx in trusted_suffixes)
        ):
            environ["wsgi.url_scheme"] = "https"
        return self.app(environ, start_response)


def configure(
    control_dir: str = ".akasa",
    trusted_namespaces: Optional[Iterable[str]] = None,
    extension_namespaces: Optional[Iterable[str]] = None,
    core_modules: Optional[Iterable[str]] = None,
    sse_passthrough: Optional[Iterable[str]] = None,
    trusted_host_suffixes: Tuple[str, ...] = (),
    tune_os_resources: bool = True,
) -> None:
    """Phase A: Headless-safe process & HookRegistry physics configuration."""
    if tune_os_resources:
        tune_process_resources()

    if control_dir:
        os.environ["AKASA_CONTROL_DIR"] = control_dir

    for ns in (trusted_namespaces or ()):
        hooks.register_trusted_namespace(ns)

    for ext_ns in (extension_namespaces or ()):
        hooks.register_extension_namespace(ext_ns)

    for mod in (core_modules or ()):
        register_core_module(mod)

    if sse_passthrough:
        hooks.register_sse_passthrough(*sse_passthrough)

    if trusted_host_suffixes:
        register_trusted_host_suffix(*trusted_host_suffixes)


def init_app(
    app: Flask,
    token_header: str = "X-Akasa-Token",
    public_routes: Optional[Iterable[str]] = None,
    public_prefixes: Tuple[str, ...] = (),
    public_suffixes: Tuple[str, ...] = (),
    sse_route: Optional[str] = None,
    enable_proxy_fix: bool = True,
    disable_browser_cache: bool = True,
) -> Flask:
    """Phase B: Wires Flask transport, authentication gate, DB connection teardown, and optional SSE stream."""
    from akasa.auth import auth_bp as _auth_bp, security_bp as _security_bp, BOOT_TOKEN as _BOOT_TOKEN
    from akasa.db import close_thread_connections as _close_thread_connections
    from akasa.events import sse_bus as _sse_bus
    import akasa.workers as _akasa_workers  # Ensure metronome hooks are registered

    _ = _akasa_workers

    if enable_proxy_fix:
        app.wsgi_app = ForceHTTPSProxyFix(app.wsgi_app)

    if "auth" not in app.blueprints:
        app.register_blueprint(_auth_bp)
    if "security" not in app.blueprints:
        app.register_blueprint(_security_bp.bp)

    allowed_routes = {"/auth/bootstrap"}
    if public_routes:
        allowed_routes.update(public_routes)
    allowed_prefixes = tuple(public_prefixes or ())
    allowed_suffixes = tuple(public_suffixes or ())

    @app.teardown_appcontext
    def _akasa_cleanup_request_db_connections(exception=None):
        _close_thread_connections()

    if disable_browser_cache:
        @app.after_request
        def _akasa_disable_http_caching(response):
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
            return response

    @app.before_request
    def _akasa_enforce_token_gate():
        path = request.path
        if (
            path in allowed_routes
            or (allowed_prefixes and path.startswith(allowed_prefixes))
            or (allowed_suffixes and path.endswith(allowed_suffixes))
        ):
            return None

        client_token = (
            request.headers.get(token_header)
            or request.headers.get("X-Akasa-Token")
            or request.args.get("token")
        )
        if not client_token or client_token != _BOOT_TOKEN:
            return jsonify({"error": "401 Unauthorized: Invalid or missing execution credentials."}), 401
        return None

    if sse_route:
        @app.route(sse_route, methods=["GET"])
        def _akasa_sse_stream():
            def event_stream():
                q = _sse_bus.subscribe()
                try:
                    yield 'event: connected\ndata: {"status": "ok"}\n\n'
                    while True:
                        try:
                            msg = q.get(timeout=5.0)
                            yield msg
                        except queue.Empty:
                            yield ": heartbeat\n\n"
                finally:
                    _sse_bus.unsubscribe(q)

            return Response(
                event_stream(),
                content_type="text/event-stream",
                headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
            )

    return app


def boot(skip_if_reloader_parent: bool = False) -> None:
    """Ignites the Akasa kernel lifecycle (DB migrations, VFS write pipeline, and Worker Metronome)."""
    import akasa.db as _akasa_db
    import akasa.vfs as _akasa_vfs
    import akasa.workers as _akasa_workers

    _ = (_akasa_db, _akasa_vfs, _akasa_workers)

    is_werkzeug_worker = os.environ.get("WERKZEUG_RUN_MAIN") == "true"
    if skip_if_reloader_parent and not is_werkzeug_worker:
        return

    try:
        hooks.emit("system_boot")
    except Exception as e:
        print(f"Warning: system_boot failed: {e}")


def reset_state(cwd: Optional[str] = None, control_dir: Optional[str] = None) -> None:
    """First-class test harness reset: closes pooled DB handles, clears RAM caches, and optionally re-anchors cwd."""
    from akasa.db import close_thread_connections as _close_thread_connections
    import akasa.vfs as _akasa_vfs
    import akasa.workers as _akasa_workers

    _close_thread_connections()
    _akasa_utils._JSON_CACHE.clear()
    _akasa_utils._JSON_MTIME.clear()
    _akasa_utils.load_config.clear_all()
    with _akasa_vfs._VFS_CACHE_LOCK:
        _akasa_vfs._VFS_READ_CACHE.clear()
        _akasa_vfs._VFS_READ_CACHE_BYTES = 0
        _akasa_vfs._VFS_PENDING_WRITES.clear()

    with _akasa_vfs._ANCESTOR_LOCK:
        _akasa_vfs._ANCESTOR_CACHE.clear()

    with _akasa_workers._SCHEMA_INIT_LOCK:
        _akasa_workers._INITIALIZED_WORKSPACES.clear()

    if cwd is not None:
        _akasa_utils.set_daemon_cwd(cwd)
    if control_dir is not None:
        os.environ["AKASA_CONTROL_DIR"] = control_dir