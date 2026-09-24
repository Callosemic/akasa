import urllib.parse
import posixpath
import re

class AkasaURI:
    """
    Zero-knowledge Universal Resource Identifier. 
    Strictly handles RFC 3986 parsing and POSIX path normalization.
    """
    def __init__(self, raw_path: str):
        if not raw_path:
            raw_path = ""
            
        clean_path = str(raw_path).replace('\\', '/').strip()
        
        if "://" in clean_path:
            scheme_part, rest = clean_path.split("://", 1)
            rest = re.sub(r'/+', '/', rest)
            clean_path = f"{scheme_part}://{rest}"
        else:
            clean_path = re.sub(r'/+', '/', clean_path)

        parsed = urllib.parse.urlparse(clean_path)
        
        self.scheme = parsed.scheme if parsed.scheme else ""
        self.volume = parsed.netloc if parsed.netloc else ""
        
        self.is_dir = clean_path.endswith('/')
        raw_internal = parsed.path
        
        normalized_path = posixpath.normpath(raw_internal)
        if normalized_path in ('.', '/'):
            normalized_path = ""
            
        self.path = normalized_path.lstrip('/')
        if self.is_dir and self.path and not self.path.endswith('/'):
            self.path += '/'

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
        if not self.path.startswith(base_uri.path):
            raise ValueError("Current URI is not a subpath of the base URI.")
        return self.path[len(base_uri.path):].lstrip('/')

    def match(self, pattern: str) -> bool:
        import fnmatch
        full_str = str(self)
        if fnmatch.fnmatch(full_str, pattern):
            return True
        if fnmatch.fnmatch(self.path, pattern):
            return True
        return False

    def __str__(self) -> str:
        if self.scheme:
            base = f"{self.scheme}://{self.volume}" if self.volume else f"{self.scheme}://"
            if self.path:
                return f"{base}/{self.path}"
            return f"{base}/" if self.is_dir else base
        elif self.volume:
            base = self.volume
            if self.path:
                return f"{base}/{self.path}"
            return f"{base}/" if self.is_dir else base
        else:
            return self.path if self.path else ("/" if self.is_dir else "")

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