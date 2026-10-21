import os
import sys
import shutil
import subprocess
from pathlib import Path
from akasa.vfs import _resolve_physical_path


def execute_binary(cmd, cwd=None, env=None, check=False, capture_output=True, text=True, popen=False, **kwargs):
    """
    Universal binary execution wrapper resolving system PATH, venv directories,
    and module CLI fallbacks across systemd, desktop, and isolated environments.
    """
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

    if popen:
        return subprocess.Popen(
            full_cmd,
            cwd=cwd,
            env=env,
            text=text,
            **kwargs
        )

    return subprocess.run(
        full_cmd,
        cwd=cwd,
        env=env,
        check=check,
        capture_output=capture_output,
        text=text,
        **kwargs
    )


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
        kwargs.setdefault('timeout', 60) # Global circuit breaker to prevent zombie threads
        return execute_binary(cmd, cwd=cwd, **kwargs)

    def popen(self, cmd, volume: str = None, **kwargs):
        """Safely spawns a detached or streaming subprocess within the sandboxed boundary."""
        cwd = self._resolve_cwd(volume, kwargs)
        return execute_binary(cmd, cwd=cwd, popen=True, **kwargs)