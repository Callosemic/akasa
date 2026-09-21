import threading
from typing import TypedDict, Optional, List, Dict, Any, Union

# --- EVENT BUS PAYLOAD CONTRACTS ---
# These TypedDicts serve as machine-readable documentation for extension authors (and LLMs)
# to guarantee exact kwarg keys when hooking into OS lifecycle events.

class HookPayload_MutateWorkspaceConfig(TypedDict, total=False):
    cfg: Dict[str, Any]
    workspace_id: Optional[str]
class HookPayload_VFSMutated(TypedDict, total=False):
    mutations: List[Dict[str, Any]]
    workspace_id: Optional[str]
class HookPayload_WorkspaceScoped(TypedDict, total=False):
    workspace_id: Optional[str]
class HookPayload_WorkspaceShutdown(TypedDict, total=False):
    workspace_id: Optional[str]

class HookPayload_RegisterManifestSignatures(TypedDict, total=False):
    workspace_id: Optional[str]
    since_ts: Optional[float]
_event_thread_local = threading.local()

class HookRegistry:
    """The central Event Bus for the Akasa Micro-Kernel."""
    def __init__(self):
        self._hooks = {}
        self._lock = threading.Lock()
        self._workspace_router = None
        self._trusted_namespaces = set()
        self._extension_namespaces = set()

    def register_trusted_namespace(self, namespace):
        """DI Interface: Host OS registers its Tier 2 core namespace for event bypass privileges."""
        self._trusted_namespaces.add(namespace)

    def register_extension_namespace(self, namespace):
        """DI Interface: Host OS registers its Tier 3 extension namespace."""
        self._extension_namespaces.add(namespace)

    def set_workspace_router(self, router_func):
        """Dependency Injection: Allows the host OS to define spatial routing."""
        self._workspace_router = router_func
    def on(self, event_name, priority=50):
        """Decorator for extensions to subscribe to OS lifecycle events."""
        def decorator(func):
            with self._lock:
                if event_name not in self._hooks:
                    self._hooks[event_name] = []

                # Guardrail: Prevent duplicate registrations from double imports
                func_file = getattr(func, '__code__', None)
                func_path = func_file.co_filename if func_file else ''
                for _, existing_func in self._hooks[event_name]:
                    existing_file = getattr(existing_func, '__code__', None)
                    existing_path = existing_file.co_filename if existing_file else ''
                    if existing_path == func_path and existing_func.__name__ == func.__name__:
                        return func

                self._hooks[event_name].append((priority, func))
                self._hooks[event_name].sort(key=lambda item: item[0])
            return func
        return decorator
    def _is_authorized(self, cb, event_name, workspace_id):
        """Boundary filter: checks if the callback's module is enabled in the active tenant scope."""
        mod = cb.__module__ or ""
        # System lifecycle and infrastructure events always bypass tenant authorization
        if event_name in ['system_shutdown', 'mutate_workspace_config', 'register_core_modules']:
            return True

        if not mod: return True

        # Core system modules (Akasa kernel or explicitly flagged core modules) are always authorized
        if mod.startswith('akasa.') or getattr(cb, 'is_core', False):
            return True

        # DI: Trusted host OS namespaces bypass tenant authorization (Tier 2 isolation)
        if any(mod.startswith(ns) for ns in self._trusted_namespaces):
            return True

        # Inversion of Control: Determine if the module belongs to an enabled extension
        # by checking if any segment of its import path matches an active extension name.
        ext_name = None
        from akasa.utils import load_config
        try:
            enabled_exts = set(load_config(workspace_id).get("extensions", []))
            for part in mod.split('.'):
                if part in enabled_exts:
                    ext_name = part
                    break
        except Exception:
            pass

        # Special Guardrail: Extensions are strictly banned from system_boot
        if event_name == 'system_boot':
            from akasa.utils import is_core_module
            # Graceful fallback: If ext_name didn't resolve, check if the module path belongs to a known core module
            if ext_name and is_core_module(ext_name):
                return True
            if any(is_core_module(p) for p in mod.split('.')):
                return True
            print(f"⚠️ [Event Bus] Banned hook: Non-core extension '{mod}' attempted to subscribe to 'system_boot'. Use 'workspace_boot' instead.")
            return False

        # Cross-reference extension engines against the central SSOT
        if ext_name:
            from akasa.utils import is_extension_enabled
            return is_extension_enabled(ext_name, workspace_id)

        return True
    def emit(self, event_name, *args, **kwargs):
        """Core OS trigger to broadcast payloads to all subscribed extensions."""
        import time
        from akasa.events import sse_bus
        # Auto-forward high-priority UI signals to the SSE stream
        if event_name in ('topology_resolved',):
            safe_payload = {k: v for k, v in kwargs.items() if isinstance(v, (str, int, float, bool, list, dict))}
            sse_bus.emit_sse(event_name, safe_payload)

        # Re-entrancy Guard: Prevent infinite recursion loops on the same thread
        if not hasattr(_event_thread_local, 'active_events'):
            _event_thread_local.active_events = set()
        if event_name in _event_thread_local.active_events:
            print(f"🛡️ [Event Bus Guardrail] Blocked recursive loop on event: '{event_name}'")
            return []

        _event_thread_local.active_events.add(event_name)
        try:
            workspace_ids = set()
            if kwargs.get('workspace_id'):
                workspace_ids.add(kwargs['workspace_id'])
            if event_name == 'vfs_mutated' and self._workspace_router:
                for m in kwargs.get('mutations', []):
                    if 'filepath' in m:
                        mapped_res = self._workspace_router(m['filepath'])
                        if mapped_res:
                            workspace_ids.update(mapped_res)

            if not workspace_ids:
                from flask import has_request_context
                if has_request_context():
                    from akasa.utils import sniff_tenant_id
                    workspace_ids.add(sniff_tenant_id())
                else:
                    workspace_ids.add("default")

            results = []
            for wid in workspace_ids:
                kwargs['workspace_id'] = wid
                with self._lock:
                    callbacks = [cb for _, cb in self._hooks.get(event_name, [])]

                for cb in callbacks:
                    if not self._is_authorized(cb, event_name, wid):
                        continue
                    try:
                        t0 = time.time()
                        results.append(cb(*args, **kwargs))
                        t1 = time.time()
                        if t1 - t0 > 1.0:
                            print(f"🐌 [Event Bus Telemetry] Hook '{event_name}' -> {cb.__module__}.{cb.__name__} took {t1 - t0:.2f}s")
                    except Exception as e:
                        print(f"⚠️ [Event Bus] Error in '{event_name}' callback {cb.__name__}: {e}")
            return results
        finally:
            _event_thread_local.active_events.remove(event_name)
    def emit_background(self, event_name, *args, **kwargs):
        """Dispatches long-running hooks to a background thread to prevent blocking."""
        workspace_ids = set()
        if kwargs.get('workspace_id'):
            workspace_ids.add(kwargs['workspace_id'])
        if event_name == 'vfs_mutated' and self._workspace_router:
            for m in kwargs.get('mutations', []):
                if 'filepath' in m:
                    # DI: Ask the registered router to spatially map the file
                    mapped_res = self._workspace_router(m['filepath'])
                    if mapped_res:
                        workspace_ids.update(mapped_res)

        if not workspace_ids:
            from flask import has_request_context
            if has_request_context():
                from akasa.utils import sniff_tenant_id
                workspace_ids.add(sniff_tenant_id())
            else:
                workspace_ids.add("default")

        def _run_hooks():
            for wid in workspace_ids:
                kwargs['workspace_id'] = wid
                with self._lock:
                    callbacks = [cb for _, cb in self._hooks.get(event_name, [])]

                for cb in callbacks:
                    if not self._is_authorized(cb, event_name, wid):
                        continue
                    try:
                        cb(*args, **kwargs)
                    except Exception as e:
                        print(f"⚠️ [Event Bus] Error in async '{event_name}' callback {cb.__name__}: {e}")

        with self._lock:
            has_callbacks = bool(self._hooks.get(event_name, []))

        if has_callbacks:
            threading.Thread(target=_run_hooks, daemon=True).start()

# The Singleton Event Bus
hooks = HookRegistry()