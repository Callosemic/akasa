"""
Bottom-layer credential store shared by akasa's token gate and a host's recovery server.

Standard library only, and no imports from the rest of akasa: a recovery server loads this file by
path (importlib.util.spec_from_file_location) so it still works when akasa's package __init__ cannot
be imported. A fitness rule (CREDENTIALS_STDLIB_ONLY) keeps it that way.

Layout, under <daemon cwd>/<control dir>/auth/ (directory 0700, files 0600):

    session_token    the durable token the gate accepts (one line, hex)

S6 adds per-device token hashes next to it; the session token stays as the break-glass credential.
"""
import hmac
import os
import secrets
import stat
import tempfile
from pathlib import Path
from typing import List, Optional, Union

AUTH_DIRNAME = "auth"
SESSION_TOKEN_FILENAME = "session_token"
TOKEN_BYTES = 32
_DIR_MODE = 0o700
_FILE_MODE = 0o600

PathLike = Union[str, Path]


def auth_dir(control_dir: PathLike) -> Path:
    """Returns <control_dir>/auth."""
    return Path(control_dir) / AUTH_DIRNAME


def session_token_path(control_dir: PathLike) -> Path:
    """Returns the path of the durable session token."""
    return auth_dir(control_dir) / SESSION_TOKEN_FILENAME


def _ensure_private_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    if os.name == "posix":
        try:
            os.chmod(path, _DIR_MODE)
        except OSError:
            pass


def _tighten(path: Path) -> None:
    """Drops group and other permission bits from an existing credential file."""
    if os.name != "posix":
        return
    try:
        mode = stat.S_IMODE(path.stat().st_mode)
        if mode & 0o077:
            os.chmod(path, _FILE_MODE)
    except OSError:
        pass


def read_token(control_dir: PathLike) -> Optional[str]:
    """Returns the stored session token, or None when it is missing, empty or unreadable."""
    path = session_token_path(control_dir)
    try:
        value = path.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeDecodeError):
        return None
    if not value:
        return None
    _tighten(path)
    return value


def _write_new(path: Path, value: str) -> None:
    """Writes value to path atomically with owner-only permissions, replacing any existing file."""
    _ensure_private_dir(path.parent)
    fd, tmp_name = tempfile.mkstemp(prefix=".tmp_", dir=path.parent.as_posix())
    try:
        if os.name == "posix":
            os.fchmod(fd, _FILE_MODE)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(value + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, path.as_posix())
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


def load_or_create_token(control_dir: PathLike) -> str:
    """
    Returns the durable session token, creating it on first use. Safe when two processes race:
    the file is created with O_EXCL, and the loser reads the winner's token.
    """
    existing = read_token(control_dir)
    if existing:
        return existing
    path = session_token_path(control_dir)
    _ensure_private_dir(path.parent)
    value = secrets.token_hex(TOKEN_BYTES)
    try:
        fd = os.open(path.as_posix(), os.O_WRONLY | os.O_CREAT | os.O_EXCL, _FILE_MODE)
    except FileExistsError:
        raced = read_token(control_dir)
        if raced:
            return raced
        _write_new(path, value)  # The existing file was empty or unreadable: replace it.
        return value
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(value + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    return value


def rotate_token(control_dir: PathLike) -> str:
    """Replaces the session token with a new one and returns it. Holders of the old token must sign in again."""
    value = secrets.token_hex(TOKEN_BYTES)
    _write_new(session_token_path(control_dir), value)
    return value


def verify_token(presented: object, expected: Optional[str]) -> bool:
    """Constant-time comparison that never accepts an empty or missing credential."""
    if not expected or not isinstance(presented, str) or not presented:
        return False
    return hmac.compare_digest(presented.encode("utf-8"), expected.encode("utf-8"))


def permission_problems(control_dir: PathLike) -> List[str]:
    """Lists credential paths that group or other users can access (POSIX only)."""
    if os.name != "posix":
        return []
    problems = []
    for path in (auth_dir(control_dir), session_token_path(control_dir)):
        try:
            mode = stat.S_IMODE(path.stat().st_mode)
        except OSError:
            continue
        if mode & 0o077:
            problems.append(f"{path.as_posix()} is accessible to other users (mode {oct(mode)})")
    return problems
