"""Tests for the durable, stdlib-only credential store and the lazy boot token built on it."""
import os
import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from flask import Flask

import akasa
import akasa.auth as akasa_auth
import akasa.utils as akasa_utils
from akasa.auth import credentials

CREDENTIALS_FILE = Path(credentials.__file__).resolve()


class TestCredentialStore(unittest.TestCase):
    """credentials.py creates, reads, rotates and verifies the session token with owner-only files."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.control = Path(self.temp_dir.name) / ".ctl"

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_token_is_created_once_and_then_reused(self):
        first = credentials.load_or_create_token(self.control)
        self.assertEqual(len(first), credentials.TOKEN_BYTES * 2)
        self.assertEqual(credentials.load_or_create_token(self.control), first)
        self.assertEqual(credentials.read_token(self.control), first)

    @unittest.skipUnless(os.name == "posix", "POSIX permissions")
    def test_files_are_owner_only(self):
        credentials.load_or_create_token(self.control)
        self.assertEqual(stat.S_IMODE(credentials.auth_dir(self.control).stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE(credentials.session_token_path(self.control).stat().st_mode), 0o600)
        self.assertEqual(credentials.permission_problems(self.control), [])

    @unittest.skipUnless(os.name == "posix", "POSIX permissions")
    def test_reading_tightens_a_world_readable_token(self):
        credentials.load_or_create_token(self.control)
        token_path = credentials.session_token_path(self.control)
        os.chmod(token_path, 0o644)
        self.assertTrue(credentials.permission_problems(self.control))
        credentials.read_token(self.control)
        self.assertEqual(stat.S_IMODE(token_path.stat().st_mode), 0o600)

    def test_empty_token_file_is_replaced(self):
        token_path = credentials.session_token_path(self.control)
        token_path.parent.mkdir(parents=True)
        token_path.write_text("\n", encoding="utf-8")
        self.assertTrue(credentials.load_or_create_token(self.control))

    def test_rotation_replaces_the_token(self):
        old = credentials.load_or_create_token(self.control)
        new = credentials.rotate_token(self.control)
        self.assertNotEqual(old, new)
        self.assertEqual(credentials.read_token(self.control), new)

    def test_verify_refuses_empty_and_wrong_credentials(self):
        token = credentials.load_or_create_token(self.control)
        self.assertTrue(credentials.verify_token(token, token))
        for presented in ("", None, 123, token + "x", token[:-1]):
            with self.subTest(presented=presented):
                self.assertFalse(credentials.verify_token(presented, token))
        self.assertFalse(credentials.verify_token("", ""))
        self.assertFalse(credentials.verify_token("anything", None))

    def test_loads_by_path_without_akasa(self):
        """The lifeboat's way of loading it: by file path, in an isolated interpreter where akasa is not importable."""
        script = (
            "import importlib.util, sys\n"
            f"spec = importlib.util.spec_from_file_location('lifeline_credentials', {CREDENTIALS_FILE.as_posix()!r})\n"
            "mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)\n"
            f"tok = mod.load_or_create_token({self.control.as_posix()!r})\n"
            "assert 'akasa' not in sys.modules, sorted(m for m in sys.modules if m.startswith('akasa'))\n"
            "print(tok)\n"
        )
        proc = subprocess.run([sys.executable, "-I", "-c", script], capture_output=True, text=True, timeout=60)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(proc.stdout.strip(), credentials.read_token(self.control))


class TestBootToken(unittest.TestCase):
    """get_boot_token: an explicit environment token wins; otherwise the durable token under the daemon's control dir."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name).resolve()
        self.orig_cwd = akasa_utils.get_daemon_cwd()
        self.orig_env_ctrl = os.environ.get("AKASA_CONTROL_DIR")
        self.env_patch = patch.dict(os.environ, {}, clear=False)
        self.env_patch.start()
        os.environ.pop("AKASA_BOOT_TOKEN", None)
        akasa.reset_state(cwd=self.root.as_posix(), control_dir=".ctl")

    def tearDown(self):
        self.env_patch.stop()
        akasa.reset_state(cwd=self.orig_cwd, control_dir=self.orig_env_ctrl or ".akasa")
        self.temp_dir.cleanup()

    def test_boot_token_is_persisted_and_survives_a_restart(self):
        token = akasa_auth.get_boot_token()
        self.assertEqual(credentials.read_token(self.root / ".ctl"), token)
        os.environ.pop("AKASA_BOOT_TOKEN")  # A fresh process has no inherited token.
        self.assertEqual(akasa_auth.get_boot_token(), token)

    def test_environment_token_wins_and_writes_nothing(self):
        os.environ["AKASA_BOOT_TOKEN"] = "explicit-override"
        self.assertEqual(akasa_auth.get_boot_token(), "explicit-override")
        self.assertFalse(credentials.session_token_path(self.root / ".ctl").exists())

    def test_legacy_attribute_names_still_resolve(self):
        self.assertEqual(akasa_auth.BOOT_TOKEN, akasa_auth.get_boot_token())
        self.assertEqual(akasa.BOOT_TOKEN, akasa_auth.get_boot_token())

    def test_gate_accepts_the_durable_token_only(self):
        app = Flask(f"gate_{id(self)}")
        akasa.init_app(app, token_header="X-Test-Token")
        app.add_url_rule("/private", "private", lambda: "ok")
        client = app.test_client()
        token = akasa_auth.get_boot_token()
        self.assertEqual(client.get("/private", headers={"Host": "localhost"}).status_code, 401)
        self.assertEqual(client.get("/private", headers={"Host": "localhost", "X-Test-Token": "wrong"}).status_code, 401)
        self.assertEqual(client.get("/private", headers={"Host": "localhost", "X-Test-Token": token}).status_code, 200)

    def test_importing_akasa_writes_nothing(self):
        script = "import os, akasa, akasa.auth; print(sorted(os.listdir('.')))"
        env = dict(os.environ, PYTHONPATH=os.pathsep.join(p for p in sys.path if p))
        env.pop("AKASA_BOOT_TOKEN", None)
        proc = subprocess.run([sys.executable, "-c", script], cwd=self.root, capture_output=True, text=True, timeout=120, env=env)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(proc.stdout.strip().splitlines()[-1], "[]")


if __name__ == "__main__":
    unittest.main()
