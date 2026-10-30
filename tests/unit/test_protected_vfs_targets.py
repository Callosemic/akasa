"""Tests for host-registered protected VFS targets: whole-segment matching and import-shadowing paths."""
import unittest

import akasa.vfs as akasa_vfs


class TestProtectedVfsTargets(unittest.TestCase):
    """register_protected_vfs_targets protects a module, everything that would shadow it, and nothing else."""

    def setUp(self):
        self.saved = set(akasa_vfs._PROTECTED_VFS_TARGETS)
        akasa_vfs._PROTECTED_VFS_TARGETS.clear()
        akasa_vfs.register_protected_vfs_targets(("hostpkg", "launcher.py"), ("kernel/auth", "store.py"))

    def tearDown(self):
        akasa_vfs._PROTECTED_VFS_TARGETS.clear()
        akasa_vfs._PROTECTED_VFS_TARGETS.update(self.saved)

    def assert_protected(self, path, expected):
        with self.subTest(path=path):
            self.assertEqual(akasa_vfs._is_s8_protected_target(path), expected)

    def test_the_file_itself_in_any_spelling(self):
        for path in (
            "vfs://repo/hostpkg/launcher.py",
            "/home/me/src/repo/hostpkg/launcher.py",
            "C:\\src\\repo\\hostpkg\\LAUNCHER.PY",
            "vfs://kernel/kernel/auth/store.py",
        ):
            self.assert_protected(path, True)

    def test_paths_that_would_shadow_it_at_import_time(self):
        for path in (
            "vfs://repo/hostpkg/launcher/__init__.py",
            "vfs://repo/hostpkg/__pycache__/launcher.cpython-313.pyc",
            "vfs://repo/hostpkg/launcher.cpython-313-x86_64-linux-gnu.so",
            "vfs://repo/hostpkg/launcher.pyc",
        ):
            self.assert_protected(path, True)

    def test_unrelated_paths_are_not_protected(self):
        for path in (
            "vfs://repo-hostpkg/launcher.py",
            "vfs://hostpkg-demo/launcher.py",
            "vfs://repo/hostpkg/sub/launcher.py",
            "vfs://repo/hostpkg/launcher_notes.md",
            "vfs://repo/hostpkg/launchers.py",
            "vfs://other/auth/store.py",
            "vfs://repo/tests/test_launcher.py",
        ):
            self.assert_protected(path, False)

    def test_refusals_name_no_host_files(self):
        result = akasa_vfs.execute_vfs_save("default", "vfs://repo/hostpkg/launcher.py", "x = 1\n", data={})
        self.assertEqual(result["status"], "error")
        self.assertNotIn("launcher", result["message"])
        result, status = akasa_vfs.execute_vfs_delete("default", "vfs://repo/hostpkg/launcher/__init__.py")
        self.assertEqual(status, 403)
        result, status = akasa_vfs.execute_vfs_move("default", "vfs://repo/hostpkg/other.py", "vfs://repo/hostpkg/launcher.py")
        self.assertEqual(status, 403)


if __name__ == "__main__":
    unittest.main()
