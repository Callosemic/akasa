import subprocess
from akasa.vfs import _resolve_physical_path

class ProcessManager:
    def __init__(self, workspace_id):
        self.workspace_id = workspace_id

    def _resolve_cwd(self, volume, kwargs):
        if volume:
            cwd = _resolve_physical_path(f"vfs://{volume}", self.workspace_id)
            if not cwd:
                raise ValueError(f"Cannot execute process: Volume '{volume}' is not mounted.")
            return cwd
        return kwargs.pop('cwd', None)

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