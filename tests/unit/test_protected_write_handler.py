"""Host policy for protected VFS targets: the write handler, confirmed writes, and execution-frame origins."""
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import akasa
import akasa.utils as akasa_utils
import akasa.vfs as akasa_vfs
import akasa.workers as akasa_workers
from akasa.context import execution_frame, get_current_origin
from akasa.vfs import (
    execute_confirmed_protected_write,
    execute_vfs_bulk,
    execute_vfs_delete,
    execute_vfs_move,
    execute_vfs_save,
    set_protected_write_handler,
    sync_vfs_barrier,
)

ABSOLUTE = {"is_absolute_artifact": True}


class TestProtectedWriteHandler(unittest.TestCase):
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
        self.saved_targets = set(akasa_vfs._PROTECTED_VFS_TARGETS)
        akasa_vfs.register_protected_vfs_targets(("hostpkg", "launcher.py"))
        self.target = self.control_dir / "hostpkg" / "launcher.py"
        self.target.parent.mkdir(parents=True)
        self.target.write_text("ORIGINAL = True\n", encoding="utf-8")
        self.requests = []

    def tearDown(self):
        set_protected_write_handler(None)
        akasa_vfs._PROTECTED_VFS_TARGETS.clear()
        akasa_vfs._PROTECTED_VFS_TARGETS.update(self.saved_targets)
        akasa.reset_state(cwd=self.orig_cwd, control_dir=self.orig_env_ctrl or ".akasa")
        try:
            self.temp_dir.cleanup()
        except OSError:
            pass

    def _hold(self, request):
        self.requests.append(request)
        return {"status": "pending_confirmation", "change_id": f"c{len(self.requests)}"}, 202

    def _content(self):
        sync_vfs_barrier("default")
        return self.target.read_text(encoding="utf-8")

    def test_without_a_handler_every_mutation_is_refused(self):
        self.assertEqual(execute_vfs_save("default", self.target.as_posix(), "X = 1\n", ABSOLUTE)["status"], "error")
        self.assertEqual(execute_vfs_delete("default", self.target.as_posix())[1], 403)
        self.assertEqual(execute_vfs_move("default", self.target.as_posix(), "/elsewhere.py")[1], 403)
        self.assertEqual(self._content(), "ORIGINAL = True\n")

    def test_the_handler_gets_the_mutation_and_its_answer_is_returned(self):
        set_protected_write_handler(self._hold)
        with execution_frame(transport="HTTP"):
            body = execute_vfs_save("default", self.target.as_posix(), "X = 1\n", ABSOLUTE)
        self.assertEqual(body, {"status": "pending_confirmation", "change_id": "c1"})
        request = self.requests[0]
        self.assertEqual((request["action"], request["content"], request["origin"]), ("save", "X = 1\n", "ui"))
        self.assertEqual(request["filepath"], self.target.as_posix())
        self.assertEqual(execute_vfs_delete("default", self.target.as_posix())[0]["change_id"], "c2")
        self.assertEqual(self.requests[1]["action"], "delete")
        self.assertEqual(self._content(), "ORIGINAL = True\n")

    def test_bulk_hands_protected_operations_to_the_handler_and_queues_the_rest(self):
        set_protected_write_handler(self._hold)
        other = self.control_dir / "hostpkg" / "notes.txt"
        body, status = execute_vfs_bulk("default", [
            {"action": "save", "filepath": self.target.as_posix(), "content": "X = 2\n", "is_absolute_artifact": True},
            {"action": "save", "filepath": other.as_posix(), "content": "fine\n", "is_absolute_artifact": True},
        ])
        sync_vfs_barrier("default")
        self.assertEqual(status, 202)
        self.assertEqual([op["filepath"] for op in body["operations"]], [other.as_posix()])
        self.assertEqual(body["protected"][0]["change_id"], "c1")
        self.assertEqual(other.read_text(encoding="utf-8"), "fine\n")
        self.assertEqual(self._content(), "ORIGINAL = True\n")

    def test_a_confirmed_write_reaches_the_file_once(self):
        set_protected_write_handler(self._hold)
        body, status = execute_confirmed_protected_write("default", self.target.as_posix(), "CONFIRMED = True\n", ABSOLUTE)
        self.assertEqual((body["status"], status), ("accepted", 202))
        self.assertEqual(self._content(), "CONFIRMED = True\n")
        self.assertEqual(self.requests, [])
        execute_vfs_save("default", self.target.as_posix(), "AFTER = True\n", ABSOLUTE)
        self.assertEqual(len(self.requests), 1, "the bypass must end with the confirmed call")
        self.assertEqual(self._content(), "CONFIRMED = True\n")

    def test_a_confirmed_write_keeps_the_other_guards(self):
        body, status = execute_confirmed_protected_write("default", "ws://other/hostpkg/launcher.py", "X = 1\n")
        self.assertEqual(body["status"], "error")
        self.assertNotEqual(status, 202)

    def test_a_transaction_hands_protected_mutations_over_at_once(self):
        set_protected_write_handler(self._hold)
        other = self.control_dir / "hostpkg" / "notes.txt"
        with akasa_vfs.VFSTransaction("default") as vfs:
            body = vfs.save(self.target.as_posix(), "X = 3\n", ABSOLUTE)
            self.assertEqual(body["change_id"], "c1", "the caller sees the handler's answer, not 'buffered'")
            self.assertEqual(vfs.delete(self.target.as_posix(), ABSOLUTE)[0]["change_id"], "c2")
            self.assertEqual(vfs.move(self.target.as_posix(), (self.control_dir / "moved.py").as_posix(), ABSOLUTE)[0]["change_id"], "c3")
            self.assertEqual(vfs.save(other.as_posix(), "fine\n", ABSOLUTE)["status"], "buffered")
        self.assertEqual([r["action"] for r in self.requests], ["save", "delete", "move"])
        self.assertEqual(self._content(), "ORIGINAL = True\n")
        self.assertEqual(other.read_text(encoding="utf-8"), "fine\n")

    def test_the_public_check_matches_the_guard(self):
        self.assertTrue(akasa_vfs.is_protected_vfs_target(self.target.as_posix()))
        self.assertTrue(akasa_vfs.is_protected_vfs_target((self.control_dir / "hostpkg" / "launcher" / "__init__.py").as_posix()))
        self.assertFalse(akasa_vfs.is_protected_vfs_target((self.control_dir / "hostpkg" / "notes.txt").as_posix()))
        self.assertFalse(akasa_vfs.is_protected_vfs_target(None))

    def test_a_confirmed_delete(self):
        _body, status = execute_confirmed_protected_write("default", self.target.as_posix(), None, ABSOLUTE)
        self.assertEqual(status, 202)
        sync_vfs_barrier("default")
        self.assertFalse(self.target.exists())


class TestExecutionFrameOrigin(unittest.TestCase):
    def test_transport_defaults_explicit_origins_and_inheritance(self):
        self.assertIsNone(get_current_origin())
        with execution_frame(transport="HTTP"):
            self.assertEqual(get_current_origin(), "ui")
            with execution_frame(transport="IPC"):
                self.assertEqual(get_current_origin(), "ui", "a nested call keeps its caller's origin")
        with execution_frame(transport="HTTP", origin="mcp"):
            with execution_frame(transport="HTTP"):
                self.assertEqual(get_current_origin(), "mcp", "an inner HTTP default never replaces an explicit origin")
            with execution_frame(origin="routine"):
                self.assertEqual(get_current_origin(), "routine")
        with execution_frame(transport="WORKER"):
            self.assertEqual(get_current_origin(), "worker")

    def test_jobs_run_with_the_submitter_origin(self):
        seen = []
        akasa_workers.register_callback("origin_probe", "capture", lambda **kwargs: seen.append(get_current_origin()))
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            (root / ".akasa" / "cache" / "db").mkdir(parents=True)
            orig_cwd = akasa_utils.get_daemon_cwd()
            orig_ctrl = os.environ.get("AKASA_CONTROL_DIR")
            akasa.reset_state(cwd=root.as_posix(), control_dir=".akasa")
            try:
                enabled = patch.object(akasa_workers, "is_extension_enabled", return_value=True)
                enabled.start()
                self.addCleanup(enabled.stop)
                with execution_frame(transport="HTTP", origin="mcp"):
                    _ws, _conn, _now, args_json = akasa_workers._prepare_job_payload({}, "default")
                self.assertEqual(json.loads(args_json)["_origin"], "mcp")
                akasa_workers._execute_job("j1", "origin_probe", "capture", 0, 0, args_json)
                self.assertEqual(seen, ["mcp"])
            finally:
                akasa.reset_state(cwd=orig_cwd, control_dir=orig_ctrl or ".akasa")


if __name__ == "__main__":
    unittest.main()
