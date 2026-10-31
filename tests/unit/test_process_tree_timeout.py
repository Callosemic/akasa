"""ProcessManager.run() and execute_binary(timeout=...) kill the whole process tree on timeout, and shell commands run as written."""
import os
import subprocess
import tempfile
import time
import unittest
from pathlib import Path

from akasa.process import ProcessManager, execute_binary


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    # A zombie still answers kill(0); treat it as gone once its parent reaped or abandoned it.
    try:
        with open(f"/proc/{pid}/stat", encoding="utf-8") as f:
            return f.read().split(")")[-1].split()[0] != "Z"
    except OSError:
        return True


@unittest.skipUnless(os.name == "posix", "process groups are POSIX-specific here")
class TestProcessTreeTimeout(unittest.TestCase):
    """A shell that backgrounds a long sleep leaves nothing running after its timeout."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.pid_file = Path(self.tmp.name) / "grandchild.pid"

    def tearDown(self):
        if self.pid_file.exists():
            try:
                os.kill(int(self.pid_file.read_text().strip()), 9)
            except (ProcessLookupError, ValueError):
                pass
        self.tmp.cleanup()

    def _script(self):
        return f"sleep 30 & echo $! > {self.pid_file}; wait"

    def _assert_grandchild_gone(self):
        pid = int(self.pid_file.read_text().strip())
        deadline = time.time() + 3
        while time.time() < deadline and _alive(pid):
            time.sleep(0.05)
        self.assertFalse(_alive(pid), "the backgrounded grandchild survived the timeout")

    def test_process_manager_run_kills_grandchildren(self):
        with self.assertRaises(subprocess.TimeoutExpired):
            ProcessManager("default").run(["bash", "-c", self._script()], cwd=self.tmp.name, timeout=1)
        self._assert_grandchild_gone()

    def test_shell_true_commands_are_covered_too(self):
        with self.assertRaises(subprocess.TimeoutExpired):
            execute_binary(self._script(), cwd=self.tmp.name, shell=True, timeout=1)
        self._assert_grandchild_gone()

    def test_normal_runs_keep_subprocess_run_semantics(self):
        res = ProcessManager("default").run(["bash", "-c", "echo out; echo err >&2; exit 3"], cwd=self.tmp.name)
        self.assertEqual((res.returncode, res.stdout, res.stderr), (3, "out\n", "err\n"))
        with self.assertRaises(subprocess.CalledProcessError):
            ProcessManager("default").run(["bash", "-c", "exit 2"], cwd=self.tmp.name, check=True)
        res = ProcessManager("default").run(["cat"], cwd=self.tmp.name, input="piped")
        self.assertEqual(res.stdout, "piped")

    def test_shell_commands_run_as_written(self):
        res = execute_binary("echo one && echo two | tr a-z A-Z", shell=True, cwd=self.tmp.name)
        self.assertEqual((res.returncode, res.stdout), (0, "one\nTWO\n"))
        res = ProcessManager("default").run("exit 4", shell=True, cwd=self.tmp.name)
        self.assertEqual(res.returncode, 4)

    def test_timeout_keeps_partial_output(self):
        with self.assertRaises(subprocess.TimeoutExpired) as caught:
            ProcessManager("default").run(["bash", "-c", "echo started; sleep 30"], cwd=self.tmp.name, timeout=1)
        self.assertIn("started", caught.exception.output or "")


if __name__ == "__main__":
    unittest.main()
