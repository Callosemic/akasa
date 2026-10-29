import os
import hmac
import queue
import threading
from typing import Optional, Iterable, Tuple, Callable
from flask import Flask, request, jsonify, Response, has_request_context
import akasa.context as _akasa_context
from akasa.context import (
    set_daemon_cwd,
    set_tenant_resolver,
    register_trusted_host_suffix,
    get_trusted_host_suffixes,
)
from akasa.events import sse_bus
from akasa.hooks import hooks
import akasa.db as _akasa_db
from akasa.db import close_thread_connections
import akasa.vfs as _akasa_vfs
import akasa.state as _akasa_state
from akasa.state import register_core_module
import akasa.workers as _akasa_workers
import akasa.extension as _akasa_extension
from akasa.auth import auth_bp, get_security_blueprint, BOOT_TOKEN
import akasa.auth.peer as _akasa_peer
from akasa.auth.peer import classify_peer, is_host_allowed, register_proxy_header, register_allowed_host

_ = _akasa_extension

try:
    import resource as _resource
except ImportError:
    _resource = None


def _flask_http_tenant_resolver() -> Optional[str]:
    """Default Ring 4 HTTP transport tenant resolver reading X-Workspace-ID from active Flask requests."""
    try:
        if has_request_context() and request:
            if "X-Workspace-ID" in request.headers:
                return request.headers["X-Workspace-ID"]
            if request.view_args and "workspace_id" in request.view_args:
                return str(request.view_args["workspace_id"])
    except Exception:
        pass
    return None


set_tenant_resolver(_flask_http_tenant_resolver)


def tune_process_resources() -> None:
    """Tier 1 OS Resource Governance: Raises file descriptor limits, bounds glibc arenas, and caps thread stack size."""
    if _resource is not None:
        try:
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
    tenant_resolver: Optional[Callable[[], Optional[str]]] = None,
    proxy_headers: Tuple[str, ...] = (),
    allowed_hosts: Tuple[str, ...] = (),
) -> None:
    """Phase A: Headless-safe process & HookRegistry physics configuration."""
    if tune_os_resources:
        tune_process_resources()

    if control_dir:
        os.environ["AKASA_CONTROL_DIR"] = control_dir

    if tenant_resolver is not None:
        set_tenant_resolver(tenant_resolver)

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

    if proxy_headers:
        register_proxy_header(*proxy_headers)

    if allowed_hosts:
        register_allowed_host(*allowed_hosts)


def init_app(
    app: Flask,
    token_header: str = "X-Akasa-Token",
    public_routes: Optional[Iterable[str]] = None,
    public_prefixes: Tuple[str, ...] = (),
    public_suffixes: Tuple[str, ...] = (),
    sse_route: Optional[str] = None,
    enable_proxy_fix: bool = True,
    disable_browser_cache: bool = True,
    enforce_host_validation: bool = True,
) -> Flask:
    """Phase B: Wires Flask transport, authentication gate, DB connection teardown, and optional SSE stream."""
    if enable_proxy_fix:
        app.wsgi_app = ForceHTTPSProxyFix(app.wsgi_app)

    if "auth" not in app.blueprints:
        app.register_blueprint(auth_bp)
    if "security" not in app.blueprints:
        app.register_blueprint(get_security_blueprint().bp)

    allowed_routes = {"/auth/bootstrap"}
    if public_routes:
        allowed_routes.update(public_routes)
    allowed_prefixes = tuple(public_prefixes or ())
    allowed_suffixes = tuple(public_suffixes or ())

    @app.teardown_appcontext
    def _akasa_cleanup_request_db_connections(exception=None):
        close_thread_connections()

    if disable_browser_cache:
        @app.after_request
        def _akasa_disable_http_caching(response):
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
            return response

    @app.before_request
    def _akasa_enforce_token_gate():
        # DNS-rebinding guard runs before the public-route allowlist so no route answers a foreign Host.
        if enforce_host_validation:
            peer = classify_peer(request.remote_addr, request.headers)
            if not is_host_allowed(peer.host, get_trusted_host_suffixes()):
                return jsonify({"error": "421 Misdirected Request: Host not allowed."}), 421
        req_route = request.path
        if (
            req_route in allowed_routes
            or (allowed_prefixes and req_route.startswith(allowed_prefixes))
            or (allowed_suffixes and req_route.endswith(allowed_suffixes))
        ):
            return None
        client_token = (
            request.headers.get(token_header)
            or request.headers.get("X-Akasa-Token")
            or request.args.get("token")
        )
        if not client_token or not hmac.compare_digest(str(client_token), str(BOOT_TOKEN)):
            return jsonify({"error": "401 Unauthorized: Invalid or missing execution credentials."}), 401
        return None

    if sse_route:
        @app.route(sse_route, methods=["GET"])
        def _akasa_sse_stream():
            def event_stream():
                q = sse_bus.subscribe()
                try:
                    yield 'event: connected\ndata: {"status": "ok"}\n\n'
                    while True:
                        try:
                            msg = q.get(timeout=5.0)
                            yield msg
                        except queue.Empty:
                            yield ": heartbeat\n\n"
                finally:
                    sse_bus.unsubscribe(q)

            return Response(
                event_stream(),
                content_type="text/event-stream",
                headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
            )

    return app
def boot(skip_if_reloader_parent: bool = False) -> None:
    """Ignites the Akasa kernel lifecycle (DB migrations, VFS write pipeline, and Worker Metronome)."""
    is_werkzeug_worker = os.environ.get("WERKZEUG_RUN_MAIN") == "true"
    if skip_if_reloader_parent and not is_werkzeug_worker:
        return

    try:
        hooks.emit("system_boot")
    except Exception as e:
        print(f"Warning: system_boot failed: {e}")
def reset_state(cwd: Optional[str] = None, control_dir: Optional[str] = None) -> None:
    """First-class test harness reset: delegates to each Ring subsystem's reset() contract."""
    # Quiesce background workers first: an in-flight job must not reopen databases or queue VFS
    # writes while the subsystems below are being reset or after the daemon cwd changes.
    _akasa_workers.reset()
    _akasa_context.reset()
    set_tenant_resolver(_flask_http_tenant_resolver)
    hooks.reset()
    _akasa_db.reset()
    _akasa_vfs.reset()
    _akasa_state.reset()
    _akasa_peer.reset()

    if cwd is not None:
        _akasa_context._in_test_sandbox = True
        set_daemon_cwd(cwd)
    if control_dir is not None:
        os.environ["AKASA_CONTROL_DIR"] = control_dir