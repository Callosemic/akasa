import json
import os
import tempfile
import unittest
from pathlib import Path

import akasa
import akasa.utils as akasa_utils
from akasa.vfs import DefaultVFSDriver, execute_vfs_save, mount_volume, register_driver
from akasa.workers import _init_worker_schema


class TestVfsResetDrain(unittest.TestCase):
    """reset_state() drains queued VFS writes first, so they land under the cwd they were queued for."""

    def _sandbox(self):
        tmp = tempfile.TemporaryDirectory()
        root = Path(tmp.name).resolve()
        control = root / ".akasa"
        (control / "cache" / "db").mkdir(parents=True, exist_ok=True)
        (root / "alpha").mkdir()
        akasa.reset_state(cwd=root.as_posix(), control_dir=".akasa")
        (control / "workspaces.json").write_text(json.dumps({"workspaces": {"default": {"config_path": "config.json"}}}), encoding="utf-8")
        (control / "config.json").write_text(json.dumps({"workspace_root": root.as_posix()}), encoding="utf-8")
        _init_worker_schema("default")
        register_driver("vfs", DefaultVFSDriver())
        mount_volume("default", "vfs", "alpha", (root / "alpha").as_posix())
        return tmp, root

    def setUp(self):
        self.orig_cwd = akasa_utils.get_daemon_cwd()
        self.orig_env_ctrl = os.environ.get("AKASA_CONTROL_DIR")

    def tearDown(self):
        akasa.reset_state(cwd=self.orig_cwd, control_dir=self.orig_env_ctrl or ".akasa")

    def test_writes_queued_before_reset_land_in_the_original_sandbox(self):
        first_tmp, first_root = self._sandbox()
        try:
            for idx in range(25):
                execute_vfs_save("default", f"vfs://alpha/note_{idx}.txt", f"payload {idx}")
            other = tempfile.TemporaryDirectory()
            try:
                akasa.reset_state(cwd=Path(other.name).resolve().as_posix(), control_dir=".akasa")
                written = sorted(p.name for p in (first_root / "alpha").glob("note_*.txt"))
                self.assertEqual(len(written), 25, "Every queued write must be flushed before reset switches cwd")
                stray = list(Path(other.name).rglob("note_*.txt"))
                self.assertEqual(stray, [], "No queued write may land under the next sandbox")
            finally:
                other.cleanup()
        finally:
            first_tmp.cleanup()


if __name__ == "__main__":
    unittest.main()
