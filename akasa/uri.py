import fnmatch
import posixpath


class AkasaURI:
    """
    Zero-knowledge Universal Resource Identifier. 
    Strictly handles RFC 3986 parsing and POSIX path normalization.
    """
    __slots__ = ('scheme', 'volume', 'path', 'is_dir', '_cached_str')

    def __init__(self, raw_path: str):
        if not raw_path:
            self.scheme = ""
            self.volume = ""
            self.path = ""
            self.is_dir = False
            self._cached_str = ""
            return

        clean_path = str(raw_path).replace('\\', '/').strip()
        if not clean_path:
            self.scheme = ""
            self.volume = ""
            self.path = ""
            self.is_dir = False
            self._cached_str = ""
            return

        self.is_dir = clean_path.endswith('/')

        if "://" not in clean_path:
            self.scheme = ""
            self.volume = ""
            stripped = clean_path.lstrip('/')
            normalized_path = posixpath.normpath(stripped) if stripped else ""
            self.path = "" if normalized_path in ('', '.', '/') else normalized_path
            if self.is_dir and self.path and not self.path.endswith('/'):
                self.path += '/'
            self._cached_str = self.path if self.path else ("/" if self.is_dir else "")
            return

        scheme_part, rest = clean_path.split("://", 1)
        self.scheme = scheme_part.lower()
        rest_stripped = rest.lstrip('/')
        if '/' in rest_stripped:
            vol_part, rel_part = rest_stripped.split('/', 1)
            self.volume = vol_part
            rel_stripped = rel_part.lstrip('/')
            normalized_path = posixpath.normpath(rel_stripped) if rel_stripped else ""
            self.path = "" if normalized_path in ('', '.', '/') else normalized_path
        else:
            self.volume = rest_stripped
            self.path = ""

        if self.is_dir and self.path and not self.path.endswith('/'):
            self.path += '/'

        base = f"{self.scheme}://{self.volume}" if self.volume else f"{self.scheme}://"
        if self.path:
            self._cached_str = f"{base}/{self.path}"
        else:
            self._cached_str = f"{base}/" if self.is_dir else base
    @property
    def parts(self) -> tuple:
        if not self.path or self.path == '/':
            return ()
        return tuple(p for p in self.path.strip('/').split('/') if p)

    @property
    def basename(self) -> str:
        if not self.path or self.path == '/':
            return ""
        return posixpath.basename(self.path.rstrip('/'))
    @property
    def parent(self) -> 'AkasaURI':
        if not self.path:
            return self
        parent_path = posixpath.dirname(self.path.rstrip('/'))
        base = f"{self.scheme}://{self.volume}" if self.scheme else self.volume
        if parent_path:
            return self.__class__(f"{base}/{parent_path}/") if base else self.__class__(f"{parent_path}/")
        return self.__class__(f"{base}/" if base else "")

    def join(self, child_path: str) -> 'AkasaURI':
        if not child_path:
            return self
        child_clean = str(child_path).replace('\\', '/').lstrip('/')
        new_path = posixpath.join(self.path, child_clean) if self.path else child_clean
        if str(child_path).replace('\\', '/').endswith('/') and not new_path.endswith('/'):
            new_path += '/'
        base = f"{self.scheme}://{self.volume}" if self.scheme else self.volume
        full = f"{base}/{new_path}" if base else new_path
        return self.__class__(full)
    def relative_to(self, base_uri: 'AkasaURI') -> str:
        if self.scheme != base_uri.scheme or self.volume != base_uri.volume:
            raise ValueError(f"URIs do not share the same scheme and volume: '{self.scheme}://{self.volume}' vs '{base_uri.scheme}://{base_uri.volume}'")
        base_clean = base_uri.path.rstrip('/')
        self_clean = self.path.rstrip('/')
        if base_clean:
            if self_clean != base_clean and not self_clean.startswith(base_clean + '/'):
                raise ValueError("Current URI is not a subpath of the base URI.")
            return self.path[len(base_clean):].lstrip('/')
        return self.path.lstrip('/')
    def match(self, pattern: str) -> bool:
        full_str = str(self)
        if fnmatch.fnmatch(full_str, pattern):
            return True
        if fnmatch.fnmatch(self.path, pattern):
            return True
        return False
    def __str__(self) -> str:
        return self._cached_str

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}('{str(self)}')"

    def __eq__(self, other) -> bool:
        if isinstance(other, AkasaURI):
            return str(self) == str(other)
        if isinstance(other, str):
            return str(self) == other
        return False

    def __hash__(self) -> int:
        return hash(str(self))

    def __fspath__(self) -> str:
        return str(self)