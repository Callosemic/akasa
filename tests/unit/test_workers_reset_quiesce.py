import json
import os
import tempfile
import time
import unittest
from pathlib import Path

import akasa
import akasa.utils as akasa_utils
import akasa.workers as akasa_workers


class TestWorkersResetQuiesce(unittest.TestCase):
    """reset_state() stops the metronome and job pools so no background thread outlives its sandbox."""

    def _sandbox(self):
        tmp = tempfile.TemporaryDirectory()
        root = Path(tmp.name).resolve()
        control = root / ".akasa"
        (control / "cache" / "db").mkdir(parents=True, exist_ok=True)
        akasa.reset_state(cwd=root.as_posix(), control_dir=".akasa")
        (control / "workspaces.json").write_text(json.dumps({"workspaces": {"default": {"config_path": "config.json"}}}), encoding="utf-8")
        (control / "config.json").write_text(json.dumps({"workspace_root": root.as_posix()}), encoding="utf-8")
        return tmp, root

    def setUp(self):
        self.orig_cwd = akasa_utils.get_daemon_cwd()
        self.orig_env_ctrl = os.environ.get("AKASA_CONTROL_DIR")

    def tearDown(self):
        akasa.reset_state(cwd=self.orig_cwd, control_dir=self.orig_env_ctrl or ".akasa")

    def test_reset_stops_metronome_and_pools_then_allows_lazy_restart(self):
        first_tmp, _ = self._sandbox()
        second = tempfile.TemporaryDirectory()
        try:
            akasa_workers.start_workers()
            self.assertTrue(akasa_workers._metronome_thread.is_alive())

            akasa.reset_state(cwd=Path(second.name).resolve().as_posix(), control_dir=".akasa")
            self.assertIsNone(akasa_workers._metronome_thread)
            self.assertIsNone(akasa_workers._executor)
            self.assertIsNone(akasa_workers._interactive_executor)
            self.assertFalse(akasa_workers._shutdown_event.is_set(), "Lazy restart must stay armed after reset")

            time.sleep(0.5)
            stray = list(Path(second.name).rglob("workers.db"))
            self.assertEqual(stray, [], "No background tick may open databases under the next sandbox")

            akasa_workers.start_workers()
            self.assertTrue(akasa_workers._metronome_thread.is_alive(), "Workers restart normally after a reset")
        finally:
            akasa.reset_state(cwd=self.orig_cwd, control_dir=self.orig_env_ctrl or ".akasa")
            second.cleanup()
            first_tmp.cleanup()


if __name__ == "__main__":
    unittest.main()
