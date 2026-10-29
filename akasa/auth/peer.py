"""
Ring 4 Peer Classification: the single source of truth for "where did this request come from".

Transport-agnostic by design: callers pass the socket peer address and a header mapping
(a Werkzeug EnvironHeaders, a plain dict, or any Mapping). No Flask import lives here.

Fail-closed rules:
- Any forwarding header (Forwarded, X-Forwarded-*, X-Real-IP, or a host-registered proxy
  header) marks the request as proxied, whatever its value says. A proxied request is never local.
- A request is direct-loopback only when the socket peer is loopback, no forwarding header is
  present, and the Host header names a loopback host.
- When a loopback proxy forwards a request, the trusted client IP is the RIGHTMOST
  X-Forwarded-For entry (the hop the local proxy appended itself). Leftmost entries are
  client-controlled and are never trusted.
"""
import ipaddress
import threading
import urllib.parse
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Optional, Tuple, Union

IPAddress = Union[ipaddress.IPv4Address, ipaddress.IPv6Address]

LOOPBACK_HOSTNAMES = frozenset({"localhost", "127.0.0.1", "::1"})

_DEFAULT_PROXY_HEADERS = (
    "forwarded",
    "x-forwarded-for",
    "x-forwarded-host",
    "x-forwarded-proto",
    "x-forwarded-port",
    "x-real-ip",
)

_REGISTRY_LOCK = threading.Lock()
_EXTRA_PROXY_HEADERS: set = set()
_ALLOWED_HOSTS: set = set()


def register_proxy_header(*names: str) -> None:
    """DI Interface: Registers additional headers whose presence marks a request as proxied (case-insensitive)."""
    with _REGISTRY_LOCK:
        for name in names:
            clean = str(name or "").strip().lower()
            if clean:
                _EXTRA_PROXY_HEADERS.add(clean)


def get_proxy_headers() -> Tuple[str, ...]:
    """Returns the lowercase forwarding header names that mark a request as proxied."""
    with _REGISTRY_LOCK:
        return _DEFAULT_PROXY_HEADERS + tuple(sorted(_EXTRA_PROXY_HEADERS))


def register_allowed_host(*hosts: str) -> None:
    """DI Interface: Registers exact hostnames (no port) accepted by Host validation, e.g. a reverse-proxy domain."""
    with _REGISTRY_LOCK:
        for host in hosts:
            clean = normalize_hostname(host)
            if clean:
                _ALLOWED_HOSTS.add(clean)


def get_allowed_hosts() -> Tuple[str, ...]:
    """Returns the registered exact-match hostnames."""
    with _REGISTRY_LOCK:
        return tuple(sorted(_ALLOWED_HOSTS))


def reset() -> None:
    """Test harness contract: clears host-registered proxy headers and allowed hosts."""
    with _REGISTRY_LOCK:
        _EXTRA_PROXY_HEADERS.clear()
        _ALLOWED_HOSTS.clear()


def parse_ip(value: Any) -> Optional[IPAddress]:
    """Parses an IP literal, tolerating IPv6 brackets, an IPv4 ':port' suffix, and IPv4-mapped IPv6."""
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw[:1] == "[":
        close_idx = raw.find("]")
        raw = raw[1:close_idx] if close_idx > 0 else raw[1:]
    elif raw.count(":") == 1:
        raw = raw.rsplit(":", 1)[0]
    try:
        addr = ipaddress.ip_address(raw)
    except ValueError:
        return None
    mapped = getattr(addr, "ipv4_mapped", None)
    return mapped if mapped is not None else addr


def is_loopback_ip(value: Any) -> bool:
    """True for 127.0.0.0/8, ::1, and IPv4-mapped loopback addresses."""
    addr = parse_ip(value)
    return bool(addr is not None and addr.is_loopback)


def normalize_hostname(value: Any) -> str:
    """Lowercases a Host header value and strips any port, IPv6 brackets, and a trailing root dot."""
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    if raw[:1] == "[":
        close_idx = raw.find("]")
        raw = raw[1:close_idx] if close_idx > 0 else raw[1:]
    elif raw.count(":") == 1:
        raw = raw.rsplit(":", 1)[0]
    return raw.rstrip(".")


def _lower_header_map(headers: Optional[Mapping]) -> dict:
    lowered: dict = {}
    if not headers:
        return lowered
    try:
        items = headers.items()
    except AttributeError:
        return lowered
    for key, value in items:
        lowered[str(key).lower()] = value
    return lowered


def rightmost_forwarded_ip(forwarded_for: Any) -> str:
    """Returns the rightmost valid IP in an X-Forwarded-For value, or '' when absent or malformed."""
    entries = [part.strip() for part in str(forwarded_for or "").split(",") if part.strip()]
    if not entries:
        return ""
    addr = parse_ip(entries[-1])
    return str(addr) if addr is not None else ""


@dataclass(frozen=True)
class PeerInfo:
    """Immutable classification of a request's origin. Build it with classify_peer()."""
    remote_addr: str
    client_ip: str
    host: str
    via_proxy: bool
    peer_is_loopback: bool
    host_is_loopback: bool

    @property
    def is_direct_loopback(self) -> bool:
        """True only for an unproxied request from this machine that names a loopback Host."""
        return self.peer_is_loopback and self.host_is_loopback and not self.via_proxy


def classify_peer(remote_addr: Any, headers: Optional[Mapping]) -> PeerInfo:
    """Classifies a request from its socket peer address and headers. Never raises."""
    lowered = _lower_header_map(headers)
    proxy_names = get_proxy_headers()
    via_proxy = any(name in lowered for name in proxy_names)
    peer_ip = parse_ip(remote_addr)
    peer_str = str(peer_ip) if peer_ip is not None else str(remote_addr or "")
    peer_is_loopback = bool(peer_ip is not None and peer_ip.is_loopback)
    host = normalize_hostname(lowered.get("host", ""))

    if via_proxy:
        # Only a proxy running on this machine is trusted to report the client address.
        client_ip = rightmost_forwarded_ip(lowered.get("x-forwarded-for")) if peer_is_loopback else peer_str
        if peer_is_loopback and not client_ip:
            real_ip = parse_ip(lowered.get("x-real-ip"))
            client_ip = str(real_ip) if real_ip is not None else ""
    else:
        client_ip = peer_str

    return PeerInfo(
        remote_addr=peer_str,
        client_ip=client_ip,
        host=host,
        via_proxy=via_proxy,
        peer_is_loopback=peer_is_loopback,
        host_is_loopback=host in LOOPBACK_HOSTNAMES,
    )


def is_host_allowed(host: Any, trusted_suffixes: Iterable[str] = (), allowed_hosts: Optional[Iterable[str]] = None) -> bool:
    """
    DNS-rebinding guard: accepts loopback hostnames, exact registered hosts, and hosts under a
    trusted suffix. A suffix such as '.example.net' matches 'a.example.net' but never 'evilexample.net'.
    """
    name = normalize_hostname(host)
    if not name:
        return False
    if name in LOOPBACK_HOSTNAMES:
        return True
    exact = set(get_allowed_hosts() if allowed_hosts is None else (normalize_hostname(h) for h in allowed_hosts))
    if name in exact:
        return True
    for suffix in trusted_suffixes or ():
        clean_sfx = str(suffix or "").strip().lower().rstrip(".")
        if not clean_sfx:
            continue
        dotted = clean_sfx if clean_sfx[:1] == "." else "." + clean_sfx
        if name.endswith(dotted) and len(name) > len(dotted):
            return True
    return False


def is_loopback_origin(url_str: Any) -> bool:
    """True when an Origin/Referer URL points at a loopback hostname (port ignored). 'null' and malformed values are False."""
    try:
        hostname = urllib.parse.urlsplit(str(url_str or "").strip()).hostname or ""
    except ValueError:
        return False
    return normalize_hostname(hostname) in LOOPBACK_HOSTNAMES
