import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from cryptography.fernet import Fernet

import akasa
import akasa.utils as akasa_utils
from akasa.extension import SettingsManager
from akasa.utils import get_daemon_host_config_path, save_json_config
from akasa.vfs import sync_vfs_barrier


DEMO_SCHEMA = [
    {"id": "ws_value", "label": "Workspace Value", "type": "text", "scope": "workspace", "default": "ws-default"},
    {"id": "daemon_value", "label": "Daemon Value", "type": "text", "scope": "daemon", "default": "d-default"},
    {"id": "repo_value", "label": "Repo Value", "type": "text", "scope": "repo", "default": "r-default"},
    {"id": "secret_value", "label": "Secret Value", "type": "password", "scope": "workspace", "secure": True, "default": ""},
]


class TestSettingsIsSet(unittest.TestCase):
    """SettingsManager.is_set distinguishes saved values from schema defaults across every scope."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root_path = Path(self.temp_dir.name).resolve()
        self.control_dir = self.root_path / ".akasa"
        (self.control_dir / "cache" / "db").mkdir(parents=True, exist_ok=True)
        self.orig_cwd = akasa_utils.get_daemon_cwd()
        self.orig_env_ctrl = os.environ.get("AKASA_CONTROL_DIR")
        akasa.reset_state(cwd=self.root_path.as_posix(), control_dir=".akasa")
        (self.control_dir / "workspaces.json").write_text(
            json.dumps({"workspaces": {"default": {"config_path": "config.json"}}}), encoding="utf-8"
        )
        (self.control_dir / "config.json").write_text(
            json.dumps({"workspace_root": self.root_path.as_posix()}), encoding="utf-8"
        )
        self.key_patch = patch("akasa.auth.get_master_key", return_value=Fernet.generate_key())
        self.key_patch.start()
        self.settings = SettingsManager("demo_ext", "default", schema=DEMO_SCHEMA)

    def tearDown(self):
        self.key_patch.stop()
        akasa.reset_state(cwd=self.orig_cwd, control_dir=self.orig_env_ctrl or ".akasa")
        try:
            self.temp_dir.cleanup()
        except OSError:
            pass

    def test_unsaved_keys_report_false_and_get_returns_defaults(self):
        for key in ("ws_value", "daemon_value", "repo_value", "secret_value"):
            with self.subTest(key=key):
                self.assertFalse(self.settings.is_set(key))
        self.assertEqual(self.settings.get("ws_value"), "ws-default")

    def test_unknown_keys_are_never_set(self):
        self.assertFalse(self.settings.is_set("not_in_schema"))

    def test_saved_values_report_true_in_each_scope(self):
        self.settings.set("ws_value", "custom")
        self.settings.set("daemon_value", "custom")
        self.settings.set("repo_value", "custom", repo="alpha")
        self.settings.set("secret_value", "s3cret")
        sync_vfs_barrier("default")
        self.assertTrue(self.settings.is_set("ws_value"))
        self.assertTrue(self.settings.is_set("daemon_value"))
        self.assertTrue(self.settings.is_set("repo_value", repo="alpha"))
        self.assertFalse(self.settings.is_set("repo_value", repo="beta"))
        self.assertTrue(self.settings.is_set("secret_value"))
        self.assertEqual(self.settings.get("secret_value"), "s3cret")

    def test_value_equal_to_default_still_counts_as_set(self):
        """Saving the default value is an explicit choice and must be distinguishable from never saving."""
        self.settings.set("ws_value", "ws-default")
        sync_vfs_barrier("default")
        self.assertTrue(self.settings.is_set("ws_value"))

    def test_daemon_root_level_value_counts_as_set(self):
        """Daemon scope reads root-level keys as a fallback, so is_set must agree with get()."""
        save_json_config(get_daemon_host_config_path(), {"daemon_value": "from-root"}, workspace_id="default")
        sync_vfs_barrier("default")
        self.assertTrue(self.settings.is_set("daemon_value"))
        self.assertEqual(self.settings.get("daemon_value"), "from-root")


if __name__ == "__main__":
    unittest.main()
