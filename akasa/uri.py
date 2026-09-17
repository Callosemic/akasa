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
            return AkasaURI(f"{base}/{parent_path}")
        return AkasaURI(base)