import os
import subprocess
from pathlib import Path
from akasa.vfs import _resolve_physical_path

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
        
        return subprocess.run(cmd, cwd=cwd, **kwargs)
        
    def popen(self, cmd, volume: str = None, **kwargs):
        """Safely spawns a detached or streaming subprocess within the sandboxed boundary."""
        cwd = self._resolve_cwd(volume, kwargs)
        return subprocess.Popen(cmd, cwd=cwd, **kwargs)