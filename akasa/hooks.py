import os
import time
import json
import inspect
import traceback
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import TypedDict, Optional, List, Dict, Any, Union, Callable

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

class HookPayload_ExecuteIdentityHandshake(TypedDict, total=False):
    request: Any
    client_ip: str
    config: Dict[str, Any]
    workspace_id: Optional[str]

class HookPayload_IsTrustedOrigin(TypedDict, total=False):
    url: str
    hostname: str
    workspace_id: Optional[str]

class HookPayload_VFSMountMissing(TypedDict, total=False):
    workspace_id: Optional[str]
    scheme: str
    volume: str

class HookPayload_PreFileSave(TypedDict, total=False):
    workspace_id: Optional[str]
    filepath: str
    resolved_path: Optional[str]
    content: str
    data: Dict[str, Any]

class HookPayload_JobBatonPassed(TypedDict, total=False):
    workspace_id: Optional[str]
    root_job_id: str
    previous_job_id: str
    next_job_id: str
    ext_name: str

class HookPayload_SystemError(TypedDict, total=False):
    workspace_id: Optional[str]
    source: str
    error_type: str
    message: str
    traceback: str
    payload: str

_event_thread_local = threading.local()
class HookRegistry:
    """The central Event Bus for the Akasa Micro-Kernel."""
    def __init__(self):
        self._hooks = {}
        self._lock = threading.Lock()
        self._bg_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="Akasa-EventBus")
        self._workspace_router = None
        self._trusted_namespaces = set()
        self._extension_namespaces = set()
        self._sse_passthrough_events = set()

    def register_sse_passthrough(self, *event_names):
        """DI Interface: Host OS registers high-priority event names to auto-forward to the SSE stream."""
        self._sse_passthrough_events.update(event_names)

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
                # Guardrail: Unwrap decorated callbacks and compare extension identity before dropping duplicates
                target_func = inspect.unwrap(func)
                func_ext = getattr(func, '_akasa_ext_name', None) or getattr(target_func, '_akasa_ext_name', None)
                func_file = getattr(target_func, '__code__', None)
                func_path = func_file.co_filename if func_file else ''
                for _, existing_func in self._hooks[event_name]:
                    existing_target = inspect.unwrap(existing_func)
                    existing_ext = getattr(existing_func, '_akasa_ext_name', None) or getattr(existing_target, '_akasa_ext_name', None)
                    existing_file = getattr(existing_target, '__code__', None)
                    existing_path = existing_file.co_filename if existing_file else ''
                    if existing_path == func_path and existing_target.__name__ == target_func.__name__ and existing_ext == func_ext:
                        return func

                self._hooks[event_name].append((priority, func))
                self._hooks[event_name].sort(key=lambda item: item[0])
            return func
        return decorator
    def _is_authorized(self, cb, event_name, workspace_id):
        """Boundary filter: checks if the callback's module is enabled in the active tenant scope."""
        target_cb = inspect.unwrap(cb)
        mod = getattr(target_cb, '__module__', None) or cb.__module__ or ""
        # System lifecycle and infrastructure events always bypass tenant authorization
        if event_name in ['system_shutdown', 'mutate_workspace_config', 'register_core_modules']:
            return True

        from akasa.utils import load_config as _load_config, is_core_module as _is_core_module, is_extension_enabled as _is_extension_enabled

        # 1. Fast O(1) Explicit Extension Stamp Check (Evaluated BEFORE akasa. module check)
        stamped_ext = getattr(cb, '_akasa_ext_name', None) or getattr(target_cb, '_akasa_ext_name', None)
        is_core_flag = getattr(cb, 'is_core', False) or getattr(target_cb, 'is_core', False)
        if is_core_flag or (stamped_ext and _is_core_module(stamped_ext)):
            return True
        if stamped_ext:
            if event_name == 'system_boot':
                print(f"⚠️ [Event Bus] Banned hook: Non-core extension '{stamped_ext}' attempted to subscribe to 'system_boot'. Use 'workspace_boot' instead.")
                return False
            return _is_extension_enabled(stamped_ext, workspace_id)

        if not mod:
            return True

        # 2. Core system modules (Akasa kernel) are always authorized
        if mod.startswith('akasa.'):
            return True

        # 3. DI: Trusted host OS namespaces bypass tenant authorization (Tier 2 isolation)
        if any(mod.startswith(ns) for ns in self._trusted_namespaces):
            return True

        # 4. Fallback: Determine if the module belongs to an enabled extension by import path
        ext_name = None
        try:
            enabled_exts = set(_load_config(workspace_id).get("extensions", []))
            for part in mod.split('.'):
                if part in enabled_exts:
                    ext_name = part
                    break
        except Exception:
            pass

        # Special Guardrail: Extensions are strictly banned from system_boot
        if event_name == 'system_boot':
            # Graceful fallback: If ext_name didn't resolve, check if the module path belongs to a known core module
            if ext_name and _is_core_module(ext_name):
                return True
            if any(_is_core_module(p) for p in mod.split('.')):
                return True
            print(f"⚠️ [Event Bus] Banned hook: Non-core extension '{mod}' attempted to subscribe to 'system_boot'. Use 'workspace_boot' instead.")
            return False

        # Cross-reference extension engines against the central SSOT
        if ext_name:
            return _is_extension_enabled(ext_name, workspace_id)

        return True
    def emit(self, event_name, *args, **kwargs):
        """Core OS trigger to broadcast payloads to all subscribed extensions."""
        from akasa.events import sse_bus as _sse_bus
        # Auto-forward registered high-priority UI signals to the SSE stream
        if event_name in self._sse_passthrough_events:
            safe_payload = {k: v for k, v in kwargs.items() if isinstance(v, (str, int, float, bool, list, dict))}
            _sse_bus.emit_sse(event_name, safe_payload)

        # Re-entrancy Guard: Prevent infinite recursion loops on the same thread
        if not hasattr(_event_thread_local, 'active_events'):
            _event_thread_local.active_events = set()
        if event_name in _event_thread_local.active_events:
            print(f"🛡️ [Event Bus Guardrail] Blocked recursive loop on event: '{event_name}'")
            return []

        _event_thread_local.active_events.add(event_name)
        try:
            workspace_ids = set()
            origin_ws = kwargs.get('workspace_id')
            if origin_ws:
                workspace_ids.add(origin_ws)

            if self._workspace_router:
                mapped_res = self._workspace_router(event_name, **kwargs)
                if mapped_res:
                    workspace_ids.update(mapped_res)

            if not workspace_ids:
                from flask import has_request_context as _has_request_context
                if _has_request_context():
                    from akasa.utils import sniff_tenant_id as _sniff_tenant_id
                    workspace_ids.add(_sniff_tenant_id())
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
                        if t1 - t0 > 5.0:
                            print(f"🐌 [Event Bus Telemetry] Hook '{event_name}' -> {cb.__module__}.{cb.__name__} took {t1 - t0:.2f}s")
                    except Exception as e:
                        print(f"⚠️ [Event Bus] Error in '{event_name}' callback {cb.__name__}: {e}")
                        try:
                            # Prevent infinite recursion if the system_error hook itself fails
                            if event_name != 'system_error':
                                safe_payload = {k: str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v for k, v in kwargs.items()}
                                self.emit_background('system_error', source=f"Hook:{event_name}:{cb.__name__}", error_type=type(e).__name__, message=str(e), traceback=traceback.format_exc(), payload=json.dumps(safe_payload), workspace_id=wid)
                        except Exception: pass
            return results
        finally:
            _event_thread_local.active_events.remove(event_name)

    def emit_first(self, event_name, *args, default=None, predicate: Optional[Callable[[Any], bool]] = None, **kwargs) -> Any:
        """Evaluates authorized callbacks in priority order and short-circuits on the first matching non-None result."""
        from akasa.events import sse_bus as _sse_bus
        if event_name in self._sse_passthrough_events:
            safe_payload = {k: v for k, v in kwargs.items() if isinstance(v, (str, int, float, bool, list, dict))}
            _sse_bus.emit_sse(event_name, safe_payload)

        if not hasattr(_event_thread_local, 'active_events'):
            _event_thread_local.active_events = set()
        if event_name in _event_thread_local.active_events:
            return default

        _event_thread_local.active_events.add(event_name)
        try:
            workspace_ids = set()
            origin_ws = kwargs.get('workspace_id')
            if origin_ws:
                workspace_ids.add(origin_ws)
            if self._workspace_router:
                mapped_res = self._workspace_router(event_name, **kwargs)
                if mapped_res:
                    workspace_ids.update(mapped_res)
            if not workspace_ids:
                from flask import has_request_context as _has_request_context
                if _has_request_context():
                    from akasa.utils import sniff_tenant_id as _sniff_tenant_id
                    workspace_ids.add(_sniff_tenant_id())
                else:
                    workspace_ids.add("default")

            check_fn = predicate if callable(predicate) else (lambda r: r is not None and r != [] and r != {})
            for wid in workspace_ids:
                kwargs['workspace_id'] = wid
                with self._lock:
                    callbacks = [cb for _, cb in self._hooks.get(event_name, [])]
                for cb in callbacks:
                    if not self._is_authorized(cb, event_name, wid):
                        continue
                    try:
                        res = cb(*args, **kwargs)
                        if check_fn(res):
                            return res
                    except Exception as e:
                        print(f"⚠️ [Event Bus] Error in '{event_name}' callback {cb.__name__}: {e}")
            return default
        finally:
            _event_thread_local.active_events.remove(event_name)

    def emit_merge_dict(self, event_name, *args, **kwargs) -> Dict[str, Any]:
        """Executes authorized callbacks and merges returned dictionaries (preserving nested None tombstones)."""
        merged: Dict[str, Any] = {}
        for res in self.emit(event_name, *args, **kwargs):
            if isinstance(res, dict):
                for k, v in res.items():
                    if isinstance(v, dict) and isinstance(merged.get(k), dict):
                        merged[k].update(v)
                    else:
                        merged[k] = v
        return merged

    def emit_merge_list(self, event_name, *args, **kwargs) -> List[Any]:
        """Executes authorized callbacks and flattens returned lists into a single list."""
        merged: List[Any] = []
        for res in self.emit(event_name, *args, **kwargs):
            if isinstance(res, list):
                merged.extend(res)
        return merged

    def emit_background(self, event_name, *args, **kwargs):
        """Dispatches long-running hooks to a background thread to prevent blocking."""
        workspace_ids = set()
        origin_ws = kwargs.get('workspace_id')
        if origin_ws:
            workspace_ids.add(origin_ws)

        if self._workspace_router:
            # DI: Ask the registered router to spatially map the event
            mapped_res = self._workspace_router(event_name, **kwargs)
            if mapped_res:
                workspace_ids.update(mapped_res)

        if not workspace_ids:
            from flask import has_request_context as _has_request_context
            if _has_request_context():
                from akasa.utils import sniff_tenant_id as _sniff_tenant_id
                workspace_ids.add(_sniff_tenant_id())
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
                        try:
                            if event_name != 'system_error':
                                safe_payload = {k: str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v for k, v in kwargs.items()}
                                self.emit_background('system_error', source=f"AsyncHook:{event_name}:{cb.__name__}", error_type=type(e).__name__, message=str(e), traceback=traceback.format_exc(), payload=json.dumps(safe_payload), workspace_id=wid)
                        except Exception: pass

        with self._lock:
            has_callbacks = bool(self._hooks.get(event_name, []))
        if has_callbacks:
            try:
                self._bg_executor.submit(_run_hooks)
            except RuntimeError:
                threading.Thread(target=_run_hooks, daemon=True).start()

# The Singleton Event Bus
hooks = HookRegistry()