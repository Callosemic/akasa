from pathlib import Path
from functools import wraps
import contextlib
import contextvars
import ctypes
import gc
import json
import os
import re
import sys
import socket
import threading
import time
import typing
import unicodedata
from akasa.uri import AkasaURI

try:
    import fcntl as _fcntl
except ImportError:
    _fcntl = None

try:
    import msvcrt as _msvcrt
except ImportError:
    _msvcrt = None

_cwd = os.getcwd()
_in_test_sandbox = False
_TRUSTED_HOST_SUFFIXES: typing.Set[str] = set()
_FILE_LOCKS_GUARD = threading.Lock()
_FILE_LOCKS: typing.Dict[str, threading.RLock] = {}
_TENANT_RESOLVER: typing.Optional[typing.Callable[[], typing.Optional[str]]] = None


class ExecutionFrame:
    """Immutable-style slotted execution frame bound to contextvars across HTTP, IPC, Worker, and CLI calls."""
    __slots__ = ("workspace_id", "transport", "method", "payload", "job_id")

    def __init__(
        self,
        workspace_id: str = "default",
        transport: str = "CLI",
        method: str = "CALL",
        payload: typing.Optional[typing.Dict[str, typing.Any]] = None,
        job_id: typing.Optional[str] = None,
    ):
        self.workspace_id = workspace_id or "default"
        self.transport = transport
        self.method = method
        self.payload = payload if isinstance(payload, dict) else {}
        self.job_id = job_id


_CURRENT_FRAME: contextvars.ContextVar[typing.Optional[ExecutionFrame]] = contextvars.ContextVar(
    "akasa_current_frame", default=None
)


@contextlib.contextmanager
def execution_frame(
    workspace_id: typing.Optional[str] = None,
    transport: str = "CLI",
    method: str = "CALL",
    payload: typing.Optional[typing.Dict[str, typing.Any]] = None,
    job_id: typing.Optional[str] = None,
):
    """Context manager binding an active ExecutionFrame for transport-agnostic tenant and payload resolution."""
    prev = _CURRENT_FRAME.get()
    resolved_ws = workspace_id or (prev.workspace_id if prev else None) or resolve_active_tenant("default")
    frame = ExecutionFrame(
        workspace_id=resolved_ws,
        transport=transport,
        method=method,
        payload=payload if payload is not None else (prev.payload if prev else None),
        job_id=job_id if job_id is not None else (prev.job_id if prev else None),
    )
    token = _CURRENT_FRAME.set(frame)
    try:
        yield frame
    finally:
        _CURRENT_FRAME.reset(token)


def get_current_frame() -> typing.Optional[ExecutionFrame]:
    """Returns the currently active ExecutionFrame on this context/thread, or None."""
    return _CURRENT_FRAME.get()


def set_tenant_resolver(resolver_fn: typing.Optional[typing.Callable[[], typing.Optional[str]]]) -> None:
    """DI Interface: Registers a transport-agnostic callable returning the active tenant workspace_id."""
    global _TENANT_RESOLVER
    _TENANT_RESOLVER = resolver_fn
def resolve_active_tenant(default: str = "default") -> str:
    """Resolves the active workspace_id from the active ExecutionFrame first, falling back to the injected tenant_resolver."""
    frame = _CURRENT_FRAME.get()
    if frame is not None and frame.workspace_id:
        return frame.workspace_id
    if _TENANT_RESOLVER is not None:
        try:
            resolved = _TENANT_RESOLVER()
            if resolved:
                return str(resolved)
        except Exception:
            pass
    return default


def sniff_tenant_id() -> str:
    """Returns the active tenant workspace_id (defaults to 'default')."""
    return resolve_active_tenant("default")


def get_control_dir_name() -> str:
    """SSOT for resolving the active host control directory name (defaults to '.akasa')."""
    return os.environ.get("AKASA_CONTROL_DIR", ".akasa")
def get_daemon_cwd() -> str:
    """Returns the anchored working directory of the Akasa daemon (honoring direct akasa.utils._cwd mutations in tests)."""
    utils_mod = sys.modules.get("akasa.utils")
    if utils_mod is not None and "_cwd" in utils_mod.__dict__:
        utils_cwd = utils_mod.__dict__["_cwd"]
        if utils_cwd != _cwd:
            return str(utils_cwd)
    return _cwd


def set_daemon_cwd(new_cwd: str) -> None:
    """Updates the anchored working directory of the Akasa daemon."""
    global _cwd
    _cwd = str(new_cwd)
    utils_mod = sys.modules.get("akasa.utils")
    if utils_mod is not None:
        utils_mod.__dict__["_cwd"] = _cwd


def register_trusted_host_suffix(*suffixes: str) -> None:
    """DI Interface: Registers trusted reverse-proxy/tunnel hostname suffixes (e.g., '.internal.net')."""
    for sfx in suffixes:
        if sfx:
            _TRUSTED_HOST_SUFFIXES.add(str(sfx))


def get_trusted_host_suffixes() -> tuple:
    """Returns the tuple of registered trusted host suffixes."""
    return tuple(_TRUSTED_HOST_SUFFIXES)


def reclaim_process_memory() -> None:
    """Tier 1 Kernel Memory Governance: Forces Python garbage collection and releases free glibc arenas back to the OS."""
    gc.collect()
    try:
        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except Exception:
        pass


def is_port_listening(port: int, host: str = "127.0.0.1") -> bool:
    """Checks if a TCP port is actively accepting connections on the given host."""
    if not port or port <= 0:
        return False
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.15)
        return s.connect_ex((host, int(port))) == 0


def can_bind_port(port: int, host: str = "0.0.0.0") -> bool:
    """Checks if a TCP port is free to bind on the given host interface."""
    if not port or port <= 0:
        return False
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind((host, int(port)))
            return True
        except OSError:
            return False


def allocate_ephemeral_port(host: str = "0.0.0.0") -> int:
    """Asks the OS kernel for an available ephemeral TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, 0))
        return int(s.getsockname()[1])


@contextlib.contextmanager
def _file_write_lock(target: Path, inter_process: bool = False):
    """Serializes concurrent writes to the same target path across threads (and optionally across OS processes for control-plane files)."""
    norm_key = target.as_posix()
    with _FILE_LOCKS_GUARD:
        thread_lock = _FILE_LOCKS.get(norm_key)
        if thread_lock is None:
            if len(_FILE_LOCKS) > 512:
                _FILE_LOCKS.clear()
            thread_lock = threading.RLock()
            _FILE_LOCKS[norm_key] = thread_lock

    with thread_lock:
        if not inter_process:
            yield
            return

        lock_path = Path(_cwd).joinpath(get_control_dir_name(), ".state.lock")
        lock_fd = None
        try:
            os.makedirs(lock_path.parent.as_posix(), exist_ok=True)
            if _fcntl is not None:
                lock_fd = os.open(lock_path.as_posix(), os.O_CREAT | os.O_RDWR, 0o600)
                _fcntl.flock(lock_fd, _fcntl.LOCK_EX)
            elif _msvcrt is not None:
                lock_fd = os.open(lock_path.as_posix(), os.O_CREAT | os.O_RDWR, 0o600)
                _msvcrt.locking(lock_fd, _msvcrt.LK_LOCK, 1)
        except OSError:
            if lock_fd is not None:
                try:
                    os.close(lock_fd)
                except OSError:
                    pass
                lock_fd = None
        try:
            yield
        finally:
            if lock_fd is not None:
                try:
                    if _fcntl is not None:
                        _fcntl.flock(lock_fd, _fcntl.LOCK_UN)
                    elif _msvcrt is not None:
                        _msvcrt.locking(lock_fd, _msvcrt.LK_UNLCK, 1)
                except OSError:
                    pass
                try:
                    os.close(lock_fd)
                except OSError:
                    pass


def atomic_write_bytes(filepath: str, data: bytes, mode: typing.Optional[int] = None, lock: bool = False) -> None:
    """
    Atomically writes bytes to filepath using a dot-prefixed sibling temporary file,
    os.fsync, permission preservation, and os.replace.
    """
    target = Path(filepath)
    parent = target.parent
    os.makedirs(parent.as_posix(), exist_ok=True)

    with _file_write_lock(target, inter_process=lock):
        existing_mode = mode
        if existing_mode is None and target.exists():
            try:
                existing_mode = target.stat().st_mode & 0o777
            except OSError:
                existing_mode = None

        tmp_name = f".tmp_{os.getpid()}_{threading.get_ident()}_{target.name}"
        tmp_path = parent.joinpath(tmp_name)
        try:
            with open(tmp_path.as_posix(), "wb") as f:
                f.write(data)
                f.flush()
                try:
                    os.fsync(f.fileno())
                except OSError:
                    pass
            if existing_mode is not None:
                try:
                    os.chmod(tmp_path.as_posix(), existing_mode)
                except OSError:
                    pass
            os.replace(tmp_path.as_posix(), target.as_posix())
        except Exception:
            if tmp_path.exists():
                try:
                    os.remove(tmp_path.as_posix())
                except OSError:
                    pass
            raise


def atomic_write_text(filepath: str, content: str, encoding: str = "utf-8", mode: typing.Optional[int] = None, lock: bool = False) -> None:
    """Atomically writes UTF-8 text to filepath via atomic_write_bytes."""
    atomic_write_bytes(filepath, content.encode(encoding), mode=mode, lock=lock)


def clone_json_value(val: typing.Any) -> typing.Any:
    """Fast recursive container cloner for JSON-compatible structures (dicts/lists) without copy.deepcopy overhead."""
    if isinstance(val, dict):
        return {k: clone_json_value(v) for k, v in val.items()}
    if isinstance(val, list):
        return [clone_json_value(item) for item in val]
    return val


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
            has_cached = False
            cached_val = None
            with state_lock:
                if key in cache and (not ttl or now - cache[key]['ts'] < ttl):
                    has_cached = True
                    cached_val = cache[key]['val']
            if has_cached:
                return dict(cached_val) if isinstance(cached_val, dict) else (list(cached_val) if isinstance(cached_val, list) else cached_val)

            key_lock = _get_key_lock(key)
            with key_lock:
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


def typeddict_to_openapi(td_class):
    """Transforms a Python TypedDict into an OpenAPI 3.0 JSON Schema object natively."""
    if not hasattr(td_class, "__annotations__"):
        return {"type": "object"}

    properties = {}
    required = []
    req_keys = getattr(td_class, '__required_keys__', td_class.__annotations__.keys())

    for key, typ in td_class.__annotations__.items():
        if key in req_keys:
            required.append(key)

        origin = typing.get_origin(typ)
        args = typing.get_args(typ)

        if origin is typing.Union and type(None) in args:
            typ = next(a for a in args if a is not type(None))
            origin = typing.get_origin(typ)
            args = typing.get_args(typ)
        if origin is typing.Literal:
            properties[key] = {"type": "string", "enum": list(args)}
        elif origin is list or origin is typing.List or typ is list:
            item_type = {"type": "string"}
            if args:
                if hasattr(args[0], '__annotations__'):
                    item_type = typeddict_to_openapi(args[0])
                else:
                    item_str = str(args[0]).lower()
                    if 'int' in item_str:
                        item_type = {"type": "integer"}
                    elif 'float' in item_str:
                        item_type = {"type": "number"}
                    elif 'bool' in item_str:
                        item_type = {"type": "boolean"}
                    elif 'dict' in item_str or 'object' in item_str:
                        item_type = {"type": "object", "additionalProperties": True}
            properties[key] = {"type": "array", "items": item_type}
        elif origin is dict or origin is typing.Dict or typ is dict:
            properties[key] = {"type": "object", "additionalProperties": True}
        elif hasattr(typ, '__annotations__'):
            properties[key] = typeddict_to_openapi(typ)
        else:
            type_str = str(typ).lower()
            if 'str' in type_str:
                properties[key] = {"type": "string"}
            elif 'int' in type_str:
                properties[key] = {"type": "integer"}
            elif 'float' in type_str:
                properties[key] = {"type": "number"}
            elif 'bool' in type_str:
                properties[key] = {"type": "boolean"}
            elif 'bytes' in type_str or 'file' in type_str:
                properties[key] = {"type": "string", "format": "binary"}
            else:
                properties[key] = {"type": "object"}

    schema = {"type": "object", "properties": properties}
    if required:
        schema["required"] = list(required)

    return schema


def generate_idempotency_hash(payload: dict) -> str:
    """Generates a deterministic JSON string hash for a dictionary payload."""
    return json.dumps(payload, sort_keys=True)


def parse_uri(path_str):
    """SDK Helper: Parses any scheme:// or relative path into (volume, relative_path)."""
    if not path_str:
        return "", ""

    uri = AkasaURI(path_str)
    if uri.scheme:
        return uri.volume, uri.path
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
        fp = ev.get('filepath') if isinstance(ev, dict) else getattr(ev, 'filepath', None)
        op = ev.get('mutation_type', ev.get('operation', 'save')) if isinstance(ev, dict) else getattr(ev, 'event_type', 'save')

        if not fp:
            continue

        op = op.lower()
        is_dir = fp.endswith('/')

        if op in ('delete', 'deleted', 'remove', 'removed'):
            dir_prefix = fp if is_dir else f"{fp}/"
            keys_to_remove = [k for k in folded.keys() if k.startswith(dir_prefix)]
            for k in keys_to_remove:
                del folded[k]

            if fp in folded:
                first_op = folded[fp].get('original_op', '')
                if first_op in ('added', 'create', 'created'):
                    del folded[fp]
                    continue

            folded[fp] = {'op': 'delete', 'event': ev, 'original_op': 'delete'}

        elif op in ('save', 'modified', 'added', 'create', 'created'):
            original_op = op
            if fp in folded and folded[fp]['op'] not in ('delete', 'deleted'):
                original_op = folded[fp].get('original_op', op)

            folded[fp] = {'op': 'save', 'event': ev, 'original_op': original_op}

    return [state['event'] for state in folded.values()]


def slugify(text):
    """Normalizes arbitrary text into a clean lowercase alphanumeric slug."""
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


def build_tree_dict(file_paths):
    """Builds a nested dictionary representing a directory hierarchy from a list of file paths."""
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
    """Renders an ASCII directory tree string from a list of file paths."""
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


def reset() -> None:
    """Resets Ring 0 context state for isolated test harnesses."""
    set_tenant_resolver(None)
    with _FILE_LOCKS_GUARD:
        _FILE_LOCKS.clear()