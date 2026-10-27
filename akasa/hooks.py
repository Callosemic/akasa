import os
import time
import json
import inspect
import traceback
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import TypedDict, Optional, List, Dict, Any, Union, Callable
from akasa.context import resolve_active_tenant, execution_frame
from akasa.events import sse_bus

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
        self._authorizer_fn: Optional[Callable[[Any, str, Optional[str], set], bool]] = None
        self._thread_cleanup_fn: Optional[Callable[[], None]] = None
        self._trusted_namespaces = set()
        self._extension_namespaces = set()
        self._sse_passthrough_events = set()
        self._auth_cache = {}

    def set_hook_authorizer(self, authorizer_fn: Callable[[Any, str, Optional[str], set], bool]) -> None:
        """DI Interface: Ring 2 state subsystem registers the tenant extension enablement evaluator."""
        with self._lock:
            self._authorizer_fn = authorizer_fn
            self._auth_cache.clear()

    def set_thread_cleanup(self, cleanup_fn: Callable[[], None]) -> None:
        """DI Interface: Ring 2 database subsystem registers thread-local connection teardown for background hook workers."""
        self._thread_cleanup_fn = cleanup_fn

    def reset(self) -> None:
        """Clears memoized hook authorization state for isolated test harnesses."""
        self.clear_auth_cache()

    def clear_auth_cache(self, workspace_id=None):
        """Purges memoized hook authorization decisions when workspace configuration mutates."""
        with self._lock:
            if workspace_id:
                self._auth_cache = {k: v for k, v in self._auth_cache.items() if k[2] != workspace_id}
            else:
                self._auth_cache.clear()

    def register_sse_passthrough(self, *event_names):
        """DI Interface: Host OS registers high-priority event names to auto-forward to the SSE stream."""
        self._sse_passthrough_events.update(event_names)

    def register_trusted_namespace(self, namespace):
        """DI Interface: Host OS registers its Tier 2 core namespace for event bypass privileges."""
        with self._lock:
            self._trusted_namespaces.add(namespace)
            self._auth_cache.clear()

    def register_extension_namespace(self, namespace):
        """DI Interface: Host OS registers its Tier 3 extension namespace."""
        with self._lock:
            self._extension_namespaces.add(namespace)
            self._auth_cache.clear()
    def set_workspace_router(self, router_func):
        """Dependency Injection: Allows the host OS to define spatial routing."""
        self._workspace_router = router_func
    def _resolve_target_workspaces(self, event_name: str, kwargs: Dict[str, Any]) -> set:
        """Resolves the target tenant workspace IDs for an event emission without coupling to Flask."""
        workspace_ids = set()
        origin_ws = kwargs.get('workspace_id')
        if origin_ws:
            workspace_ids.add(origin_ws)

        if self._workspace_router:
            mapped_res = self._workspace_router(event_name, **kwargs)
            if mapped_res:
                workspace_ids.update(mapped_res)

        if not workspace_ids:
            workspace_ids.add(resolve_active_tenant("default"))

        return workspace_ids

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
        """Boundary filter: delegates to the registered Ring 2 state authorizer while enforcing Ring 1 namespace rules."""
        if event_name in ('system_shutdown', 'mutate_workspace_config', 'register_core_modules'):
            return True
        if self._authorizer_fn is not None:
            return self._authorizer_fn(cb, event_name, workspace_id, self._trusted_namespaces)
        return True
    def _is_authorized_cached(self, cb, event_name, workspace_id):
        if event_name in ('config_mutated', 'system_shutdown', 'mutate_workspace_config', 'register_core_modules'):
            return True
        cache_key = (cb, event_name, workspace_id)
        cached = self._auth_cache.get(cache_key)
        if cached is not None:
            return cached
        decision = self._is_authorized(cb, event_name, workspace_id)
        with self._lock:
            self._auth_cache[cache_key] = decision
        return decision
    def emit(self, event_name, *args, **kwargs):
        """Core OS trigger to broadcast payloads to all subscribed extensions."""
        if event_name == 'config_mutated':
            self.clear_auth_cache(kwargs.get('workspace_id'))
        # Auto-forward registered high-priority UI signals to the SSE stream
        if event_name in self._sse_passthrough_events:
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
            workspace_ids = self._resolve_target_workspaces(event_name, kwargs)

            results = []
            for wid in workspace_ids:
                kwargs['workspace_id'] = wid
                with self._lock:
                    callbacks = [cb for _, cb in self._hooks.get(event_name, [])]
                for cb in callbacks:
                    if not self._is_authorized_cached(cb, event_name, wid):
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
        if event_name in self._sse_passthrough_events:
            safe_payload = {k: v for k, v in kwargs.items() if isinstance(v, (str, int, float, bool, list, dict))}
            sse_bus.emit_sse(event_name, safe_payload)

        if not hasattr(_event_thread_local, 'active_events'):
            _event_thread_local.active_events = set()
        if event_name in _event_thread_local.active_events:
            return default
        _event_thread_local.active_events.add(event_name)
        try:
            workspace_ids = self._resolve_target_workspaces(event_name, kwargs)

            check_fn = predicate if callable(predicate) else (lambda r: r is not None and r != [] and r != {})
            for wid in workspace_ids:
                kwargs['workspace_id'] = wid
                with self._lock:
                    callbacks = [cb for _, cb in self._hooks.get(event_name, [])]
                for cb in callbacks:
                    if not self._is_authorized_cached(cb, event_name, wid):
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
    def sync_event_bus_barrier(self, timeout: float = 5.0) -> bool:
        """Waits for all currently queued Akasa-EventBus background hook tasks to complete."""
        done_events = [threading.Event() for _ in range(4)]
        try:
            for ev in done_events:
                self._bg_executor.submit(ev.set)
        except RuntimeError:
            return True
        deadline = time.time() + timeout
        for ev in done_events:
            remaining = max(0.01, deadline - time.time())
            if not ev.wait(timeout=remaining):
                return False
        return True
    def emit_background(self, event_name, *args, **kwargs):
        """Dispatches long-running hooks to a background thread to prevent blocking."""
        if event_name == 'config_mutated':
            self.clear_auth_cache(kwargs.get('workspace_id'))
        if event_name in self._sse_passthrough_events:
            safe_payload = {k: v for k, v in kwargs.items() if isinstance(v, (str, int, float, bool, list, dict))}
            sse_bus.emit_sse(event_name, safe_payload)

        workspace_ids = self._resolve_target_workspaces(event_name, kwargs)

        def _run_hooks():
            try:
                for wid in workspace_ids:
                    with execution_frame(workspace_id=wid, transport="WORKER"):
                        kwargs['workspace_id'] = wid
                        with self._lock:
                            callbacks = [cb for _, cb in self._hooks.get(event_name, [])]
                        for cb in callbacks:
                            if not self._is_authorized_cached(cb, event_name, wid):
                                continue
                            try:
                                cb(*args, **kwargs)
                            except Exception as e:
                                print(f"⚠️ [Event Bus] Error in async '{event_name}' callback {cb.__name__}: {e}")
                                try:
                                    if event_name != 'system_error':
                                        safe_payload = {k: str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v for k, v in kwargs.items()}
                                        self.emit_background('system_error', source=f"AsyncHook:{event_name}:{cb.__name__}", error_type=type(e).__name__, message=str(e), traceback=traceback.format_exc(), payload=json.dumps(safe_payload), workspace_id=wid)
                                except Exception:
                                    pass
            finally:
                if self._thread_cleanup_fn is not None:
                    self._thread_cleanup_fn()

        with self._lock:
            has_callbacks = bool(self._hooks.get(event_name, []))
        if has_callbacks:
            try:
                self._bg_executor.submit(_run_hooks)
            except RuntimeError:
                threading.Thread(target=_run_hooks, daemon=True).start()

# The Singleton Event Bus
hooks = HookRegistry()