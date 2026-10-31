import os
import signal
import sys
import shlex
import shutil
import subprocess
from pathlib import Path
from akasa.vfs import _resolve_physical_path


def execute_binary(cmd, cwd=None, env=None, check=False, capture_output=True, text=True, popen=False, **kwargs):
    """
    Universal binary execution wrapper resolving system PATH, venv directories,
    and module CLI fallbacks across systemd, desktop, and isolated environments.
    """
    if kwargs.get("shell"):
        # The shell parses the command line itself, so it is passed through as one string, unresolved.
        # (Resolving its first word as a binary turned "a && b" into `python -m "a && b"`.)
        full_cmd = cmd if isinstance(cmd, str) else shlex.join(str(part) for part in cmd)
        if not full_cmd.strip():
            raise ValueError("Command cannot be empty.")
        return _spawn(full_cmd, cwd, env, check, capture_output, text, popen, **kwargs)

    cmd_args = [cmd] if isinstance(cmd, str) else list(cmd)
    if not cmd_args:
        raise ValueError("Command list cannot be empty.")

    bin_name = cmd_args[0]
    resolved_bin = shutil.which(bin_name)

    if not resolved_bin:
        py_dir = Path(sys.executable).parent
        ext = '.exe' if os.name == 'nt' else ''
        candidate = py_dir / f"{bin_name}{ext}"
        if candidate.exists():
            resolved_bin = candidate.as_posix()
    if resolved_bin:
        full_cmd = [resolved_bin] + cmd_args[1:]
    else:
        module_name = bin_name.replace('-', '_')
        full_cmd = [sys.executable, '-m', module_name] + cmd_args[1:]
    return _spawn(full_cmd, cwd, env, check, capture_output, text, popen, **kwargs)


def _spawn(full_cmd, cwd, env, check, capture_output, text, popen, **kwargs):
    """Starts a resolved command with the boot token removed from its environment."""
    child_env = dict(os.environ) if env is None else dict(env)
    child_env.pop("AKASA_BOOT_TOKEN", None)
    if popen:
        if capture_output:
            kwargs.setdefault("stdout", subprocess.PIPE)
            kwargs.setdefault("stderr", subprocess.PIPE)
        return subprocess.Popen(
            full_cmd,
            cwd=cwd,
            env=child_env,
            text=text,
            **kwargs
        )

    if kwargs.get("timeout") is None:
        return subprocess.run(
            full_cmd,
            cwd=cwd,
            env=child_env,
            check=check,
            capture_output=capture_output,
            text=text,
            **kwargs
        )
    return _run_with_tree_timeout(full_cmd, cwd, child_env, check, capture_output, text, **kwargs)


def _kill_process_tree(proc) -> None:
    """Kills a child and everything it started. The child leads its own session (POSIX) or process group (Windows)."""
    if proc.poll() is not None:
        return
    try:
        if os.name == "posix":
            os.killpg(proc.pid, signal.SIGKILL)
        else:
            subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)], capture_output=True)
    except (ProcessLookupError, PermissionError, OSError):
        pass
    try:
        proc.kill()
    except OSError:
        pass


def _run_with_tree_timeout(full_cmd, cwd, env, check, capture_output, text, timeout=None, input=None, **kwargs):
    """
    subprocess.run() semantics, except that a timeout kills the whole process tree. subprocess.run() only kills
    the direct child, so grandchildren of a shell, test runner or build tool survive holding locks and ports.
    The child is started as the leader of a new session (POSIX) or process group (Windows) so the tree can be killed.
    """
    if capture_output:
        kwargs.setdefault("stdout", subprocess.PIPE)
        kwargs.setdefault("stderr", subprocess.PIPE)
    if input is not None:
        kwargs["stdin"] = subprocess.PIPE
    if os.name == "posix":
        if "preexec_fn" not in kwargs:
            kwargs.setdefault("start_new_session", True)
    else:
        kwargs.setdefault("creationflags", getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0))

    with subprocess.Popen(full_cmd, cwd=cwd, env=env, text=text, **kwargs) as proc:
        try:
            stdout, stderr = proc.communicate(input, timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            _kill_process_tree(proc)
            try:
                stdout, stderr = proc.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                # A descendant that escaped the group still holds the pipes; stop waiting for it.
                stdout, stderr = exc.output, exc.stderr
            raise subprocess.TimeoutExpired(proc.args, timeout, output=stdout, stderr=stderr) from None
        except BaseException:
            _kill_process_tree(proc)
            raise
        retcode = proc.poll()
    if check and retcode:
        raise subprocess.CalledProcessError(retcode, proc.args, output=stdout, stderr=stderr)
    return subprocess.CompletedProcess(proc.args, retcode, stdout, stderr)


class ProcessManager:
    def __init__(self, workspace_id):
        self.workspace_id = workspace_id

    def _resolve_cwd(self, volume, kwargs):
        cwd = kwargs.pop('cwd', None)
        if cwd is not None:
            return str(cwd)
        if volume:
            volume_str = str(volume)
            if os.path.isabs(volume_str) or Path(volume_str).is_absolute():
                return volume_str
            resolved = _resolve_physical_path(f"vfs://{volume_str}", self.workspace_id)
            if resolved:
                return resolved
            if os.path.exists(volume_str):
                return volume_str
            raise ValueError(f"Cannot execute process: Volume '{volume}' is not mounted.")
        return None

    def run(self, cmd, volume: str = None, **kwargs):
        """Safely executes a subprocess within the sandboxed boundary of a mounted volume."""
        cwd = self._resolve_cwd(volume, kwargs)
        
        # Centralized Kernel Guardrails
        kwargs.setdefault('capture_output', True)
        kwargs.setdefault('text', True)
        kwargs.setdefault('timeout', 60) # Global circuit breaker; on expiry the whole process tree is killed
        return execute_binary(cmd, cwd=cwd, **kwargs)

    def popen(self, cmd, volume: str = None, **kwargs):
        """Safely spawns a detached or streaming subprocess within the sandboxed boundary."""
        cwd = self._resolve_cwd(volume, kwargs)
        return execute_binary(cmd, cwd=cwd, popen=True, **kwargs)