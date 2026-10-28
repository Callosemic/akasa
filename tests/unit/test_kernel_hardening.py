import ast
import contextlib
import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import akasa
import akasa.context as akasa_context
import akasa.state as akasa_state
import akasa.utils as akasa_utils
from akasa.context import (
    execution_frame,
    get_current_frame,
    sniff_tenant_id,
    thread_safe_cache,
)
from akasa.db import get_connection
from akasa.extension import AkasaExtension
from akasa.hooks import hooks
from akasa.process import execute_binary
from akasa.state import load_json_config, save_json_config
from akasa.uri import AkasaURI
from akasa.vfs import (
    DefaultVFSDriver,
    execute_vfs_save,
    mount_volume,
    register_driver,
    sync_vfs_barrier,
)
from akasa.workers import _init_worker_schema
from tests.fitness.core import FitnessState, KERNEL_DIR
from tests.fitness.rules_python import KernelFitnessVisitor, check_python_files


class TestAkasaKernelHardening(unittest.TestCase):
    """Behavioral unit tests for RFC 0048 Akasa Micro-Kernel Hardening (Refactors A–F)."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root_path = Path(self.temp_dir.name).resolve()
        self.control_dir = self.root_path / ".akasa"
        (self.control_dir / "cache" / "db").mkdir(parents=True, exist_ok=True)
        self.repo_dir = self.root_path / "alpha"
        self.repo_dir.mkdir(parents=True, exist_ok=True)

        self.orig_cwd = akasa_utils.get_daemon_cwd()
        self.orig_env_ctrl = os.environ.get("AKASA_CONTROL_DIR")
        akasa.reset_state(cwd=self.root_path.as_posix(), control_dir=".akasa")

        self.workspace_id = "default"
        (self.control_dir / "workspaces.json").write_text(
            json.dumps({"workspaces": {"default": {"config_path": "config.json"}}}),
            encoding="utf-8",
        )
        (self.control_dir / "config.json").write_text(
            json.dumps({
                "workspace_root": self.root_path.as_posix(),
                "extensions": ["enabled_ext"],
            }),
            encoding="utf-8",
        )
        _init_worker_schema(self.workspace_id)
        register_driver("vfs", DefaultVFSDriver())
        mount_volume(self.workspace_id, "vfs", "alpha", self.repo_dir.as_posix())

    def tearDown(self):
        akasa.reset_state(cwd=self.orig_cwd, control_dir=self.orig_env_ctrl or ".akasa")
        try:
            self.temp_dir.cleanup()
        except OSError:
            pass

    def test_refactor_c_akasa_uri_slots_and_fast_path_parity(self):
        """AkasaURI uses __slots__ (no instance __dict__) and normalizes schemed and relative paths without POSIX // leaks."""
        uri = AkasaURI("vfs://alpha//nested///module.py")
        self.assertFalse(hasattr(uri, "__dict__"), "AkasaURI must use __slots__ without allocating __dict__")
        self.assertEqual(uri.scheme, "vfs")
        self.assertEqual(uri.volume, "alpha")
        self.assertEqual(uri.path, "nested/module.py")
        self.assertEqual(uri.basename, "module.py")
        self.assertFalse(uri.is_dir)
        self.assertEqual(str(uri), "vfs://alpha/nested/module.py")

        # Verify double-leading-slash relative path does not leak '//' from posixpath.normpath
        rel_uri = AkasaURI("//folder///sub/item.txt")
        self.assertEqual(rel_uri.scheme, "")
        self.assertEqual(rel_uri.volume, "")
        self.assertEqual(rel_uri.path, "folder/sub/item.txt")
        self.assertEqual(str(rel_uri), "folder/sub/item.txt")

        # Verify trailing-slash directory semantics on root volume and subpaths
        dir_uri = AkasaURI("vfs://alpha/docs/")
        self.assertTrue(dir_uri.is_dir)
        self.assertEqual(dir_uri.path, "docs/")
        self.assertEqual(str(dir_uri), "vfs://alpha/docs/")

        root_vol_dir = AkasaURI("vfs://alpha/")
        self.assertTrue(root_vol_dir.is_dir)
        self.assertEqual(root_vol_dir.volume, "alpha")
        self.assertEqual(root_vol_dir.path, "")
        self.assertEqual(str(root_vol_dir), "vfs://alpha/")

    def test_refactor_d_thread_safe_cache_zero_copy_isolation(self):
        """@thread_safe_cache isolates caller mutations from cached dicts/lists and supports targeted invalidation."""
        call_count = 0

        @thread_safe_cache(ttl=10.0, key_maker=lambda k="a": k)
        def compute_payload(k="a"):
            nonlocal call_count
            call_count += 1
            return {"key": k, "count": call_count}

        first = compute_payload("alpha")
        self.assertEqual(first["count"], 1)
        first["key"] = "mutated_by_caller"

        second = compute_payload("alpha")
        self.assertEqual(call_count, 1, "Cache hit must not re-invoke underlying function")
        self.assertEqual(second["key"], "alpha", "Caller mutation must not corrupt cached dictionary")

        compute_payload.invalidate("alpha")
        third = compute_payload("alpha")
        self.assertEqual(call_count, 2, "Invalidated key must recompute on next read")
        self.assertEqual(third["count"], 2)

    def test_refactor_e_hook_auth_memoization_and_config_invalidation(self):
        """HookRegistry memoizes authorization in _auth_cache and self-invalidates when config_mutated fires."""
        hooks.clear_auth_cache()
        enabled_bp = AkasaExtension("enabled_ext", __name__, core=False)
        disabled_bp = AkasaExtension("disabled_ext", __name__, core=False)
        invocations = []

        @enabled_bp.on("custom_test_event")
        def _on_enabled(workspace_id=None, **kwargs):
            invocations.append("enabled_ext")
            return "from_enabled"

        @disabled_bp.on("custom_test_event")
        def _on_disabled(workspace_id=None, **kwargs):
            invocations.append("disabled_ext")
            return "from_disabled"

        res1 = hooks.emit("custom_test_event", workspace_id=self.workspace_id)
        self.assertEqual(res1, ["from_enabled"])
        self.assertIn((_on_enabled, "custom_test_event", self.workspace_id), hooks._auth_cache)
        self.assertIn((_on_disabled, "custom_test_event", self.workspace_id), hooks._auth_cache)
        self.assertFalse(hooks._auth_cache[(_on_disabled, "custom_test_event", self.workspace_id)])

        # Enable 'disabled_ext' in config.json and emit 'config_mutated' -> must purge _auth_cache and authorize _on_disabled
        (self.control_dir / "config.json").write_text(
            json.dumps({
                "workspace_root": self.root_path.as_posix(),
                "extensions": ["enabled_ext", "disabled_ext"],
            }),
            encoding="utf-8",
        )
        hooks.emit("config_mutated", workspace_id=self.workspace_id)
        self.assertEqual(len(hooks._auth_cache), 0, "emitting config_mutated must clear _auth_cache for the workspace")

        res2 = hooks.emit("custom_test_event", workspace_id=self.workspace_id)
        self.assertIn("from_enabled", res2)
        self.assertIn("from_disabled", res2)

    def test_refactor_a_and_b_micro_batched_vfs_commits_and_idempotency(self):
        """Micro-batched VFS queue drains bursts into coalesced vfs_event_log commits and drops idempotent writes."""
        mutation_batches = []

        @hooks.on("vfs_mutated")
        def _capture_mutations(mutations=None, workspace_id=None, **kwargs):
            if workspace_id == self.workspace_id and mutations:
                mutation_batches.append(list(mutations))

        # 1. Burst 20 file writes + barrier
        for idx in range(20):
            execute_vfs_save(self.workspace_id, f"vfs://alpha/file_{idx}.txt", f"payload_{idx}")
        sync_vfs_barrier(self.workspace_id)

        conn = get_connection("workers", workspace_id=self.workspace_id)
        logged_rows = conn.execute("SELECT filepath FROM vfs_event_log").fetchall()
        self.assertEqual(len(logged_rows), 20, "All 20 files must be recorded in vfs_event_log before barrier releases")
        self.assertLess(len(mutation_batches), 20, "Micro-batching must coalesce concurrent writes into fewer vfs_mutated emissions")

        # 2. Idempotency check: re-saving identical content must be dropped without emitting new mutations
        batches_before = len(mutation_batches)
        target_file = self.repo_dir / "file_0.txt"
        mtime_before = target_file.stat().st_mtime_ns

        execute_vfs_save(self.workspace_id, "vfs://alpha/file_0.txt", "payload_0")
        sync_vfs_barrier(self.workspace_id)

        self.assertEqual(len(mutation_batches), batches_before, "Idempotent save must not emit vfs_mutated")
        self.assertEqual(target_file.stat().st_mtime_ns, mtime_before, "Idempotent save must not rewrite physical disk file")

    def test_refactor_f_sqlite_wal_and_temp_store_pragmas(self):
        """get_connection configures WAL journal_mode and MEMORY temp_store (2)."""
        conn = get_connection("workers", workspace_id=self.workspace_id)
        journal_mode = conn.execute("PRAGMA journal_mode;").fetchone()[0]
        temp_store = conn.execute("PRAGMA temp_store;").fetchone()[0]
        self.assertEqual(str(journal_mode).lower(), "wal")
        self.assertEqual(int(temp_store), 2)

    def test_subprocess_token_sanitization(self):
        """execute_binary purges AKASA_BOOT_TOKEN from child environments in run and popen modes."""
        os.environ["AKASA_BOOT_TOKEN"] = "token_in_os_environ"
        try:
            # 1. Inherited os.environ
            res_inherited = execute_binary([
                sys.executable, "-c", "import os; print(os.environ.get('AKASA_BOOT_TOKEN', 'PURGED'))"
            ])
            self.assertEqual(res_inherited.stdout.strip(), "PURGED")

            # 2. Explicit custom env dict
            custom_env = {"AKASA_BOOT_TOKEN": "token_in_custom_env", "PATH": os.environ.get("PATH", "")}
            res_custom = execute_binary([
                sys.executable, "-c", "import os; print(os.environ.get('AKASA_BOOT_TOKEN', 'PURGED'))"
            ], env=custom_env)
            self.assertEqual(res_custom.stdout.strip(), "PURGED")

            # 3. Popen streaming mode with custom env
            proc = execute_binary([
                sys.executable, "-c", "import os; print(os.environ.get('AKASA_BOOT_TOKEN', 'PURGED'))"
            ], env=custom_env, popen=True)
            stdout, _ = proc.communicate()
            self.assertEqual(stdout.strip(), "PURGED")
        finally:
            os.environ.pop("AKASA_BOOT_TOKEN", None)

    def test_fitness_rule_catches_duplicate_top_level_definitions(self):
        """KernelFitnessVisitor detects and reports DUPLICATE_TOP_LEVEL_DEFINITION on duplicate functions."""
        code_with_dupe = (
            "def sample_target():\n"
            "    return 1\n"
            "\n"
            "def sample_target():\n"
            "    return 2\n"
        )
        tree = ast.parse(code_with_dupe, filename="sample_module.py")
        init_violations = FitnessState.violations_found
        visitor = KernelFitnessVisitor("sample_module.py", "sample_module.py")
        visitor.visit(tree)
        self.assertGreater(
            FitnessState.violations_found,
            init_violations,
            "KernelFitnessVisitor must record a violation for duplicate top-level function definitions."
        )

    def test_json_config_pending_write_read_your_writes_and_locking(self):
        """save_json_config guarantees immediate Read-Your-Writes in load_json_config even before physical disk flush."""
        new_cfg_path = (self.control_dir / "brand_new.settings.json").as_posix()
        self.assertFalse(os.path.exists(new_cfg_path))

        payload = {"feature_flag": True, "nested": {"count": 42}}
        akasa_utils.save_json_config(new_cfg_path, payload, workspace_id=self.workspace_id)

        # Immediately read back BEFORE sync_vfs_barrier flushes to disk
        immediate_read = akasa_utils.load_json_config(new_cfg_path, {})
        self.assertEqual(immediate_read, payload, "load_json_config must return staged pending JSON even before disk creation")

        # Mutate returned dict and verify internal cache is not corrupted
        immediate_read["feature_flag"] = False
        second_read = akasa_utils.load_json_config(new_cfg_path, {})
        self.assertTrue(second_read["feature_flag"], "Caller mutation must not corrupt cached JSON state")

        sync_vfs_barrier(self.workspace_id)
        self.assertTrue(os.path.exists(new_cfg_path))
        post_flush_read = akasa_utils.load_json_config(new_cfg_path, {})
        self.assertEqual(post_flush_read, payload)
    def test_injectable_tenant_resolver_decoupling(self):
        """akasa.configure(tenant_resolver=...) and execution_frame route sniff_tenant_id and HookRegistry emissions without Flask."""
        with execution_frame(workspace_id="frame_scoped_tenant", transport="CLI"):
            self.assertEqual(sniff_tenant_id(), "frame_scoped_tenant")
            self.assertEqual(get_current_frame().transport, "CLI")

        akasa.configure(tenant_resolver=lambda: "custom_headless_tenant")
        self.assertEqual(sniff_tenant_id(), "custom_headless_tenant")

        # Explicit execution_frame must take precedence over ambient tenant_resolver, and restore on exit
        with execution_frame(workspace_id="explicit_inner_tenant", transport="IPC"):
            self.assertEqual(sniff_tenant_id(), "explicit_inner_tenant")
            with execution_frame(workspace_id="deep_nested_tenant", transport="WORKER"):
                self.assertEqual(sniff_tenant_id(), "deep_nested_tenant")
            self.assertEqual(sniff_tenant_id(), "explicit_inner_tenant")
        self.assertEqual(sniff_tenant_id(), "custom_headless_tenant")

        captured_tenants = []

        @hooks.on("headless_tenant_probe")
        def _probe_listener(workspace_id=None, **kwargs):
            captured_tenants.append(workspace_id)
            return workspace_id

        res = hooks.emit_first("headless_tenant_probe")
        self.assertEqual(res, "custom_headless_tenant")
        self.assertEqual(captured_tenants, ["custom_headless_tenant"])

    def test_ring2_parsed_json_cache_avoids_redundant_json_loads(self):
        """load_json_config caches parsed objects keyed by VFS causal revision token and avoids redundant json.loads calls on cache hits."""
        cfg_file = (self.control_dir / "config.json").as_posix()

        # Verify utils facade re-exports Ring 0 and Ring 2 primitives identically
        self.assertIs(akasa_utils.load_json_config, load_json_config)
        self.assertIs(akasa_utils.save_json_config, save_json_config)
        self.assertIs(akasa_utils.execution_frame, execution_frame)

        # First read populates the parsed cache
        first = load_json_config(cfg_file, {})
        self.assertIn("workspace_root", first)

        # Subsequent reads of unchanged file must hit _JSON_PARSED_CACHE without calling json.loads
        with patch("akasa.state.json.loads", wraps=json.loads) as spy_loads:
            for _ in range(10):
                cached = load_json_config(cfg_file, {})
                self.assertEqual(cached["extensions"], ["enabled_ext"])
            self.assertEqual(spy_loads.call_count, 0, "Cache hit must return cloned parsed object without calling json.loads")

    def test_job_manager_write_scratch_registers_ephemeral_artifact(self):
        """ctx.jobs.write_scratch writes scratch content to the extension scratch cache and registers it in ephemeral_artifacts."""
        ext_bp = AkasaExtension("enabled_ext", __name__, core=False)
        ctx = ext_bp.get_context(self.workspace_id)
        scratch_path = ctx.jobs.write_scratch("node_output.txt", "oversized scratch payload", ttl_seconds=1800)

        self.assertTrue(os.path.exists(scratch_path))
        self.assertEqual(Path(scratch_path).read_text(encoding="utf-8"), "oversized scratch payload")
        self.assertIn(scratch_path, ctx.jobs.get_ephemerals())

    def test_fitness_rules_enforce_ring_dag_and_transport_isolation(self):
        """KernelFitnessVisitor enforces zero violations on the live kernel and catches upward Ring imports, Flask leaks, and inline imports."""
        prev = FitnessState.violations_found
        FitnessState.violations_found = 0
        buf = io.StringIO()
        try:
            with contextlib.redirect_stdout(buf):
                check_python_files()
            self.assertEqual(
                FitnessState.violations_found,
                0,
                f"Live Akasa kernel had fitness violations:\n{buf.getvalue()}",
            )

            # 1. Ring 0 (context.py) importing upward from Ring 2 (vfs) and Ring 4 (extension)
            bad_ring0_code = (
                "import akasa.extension\n"
                "from akasa.vfs import execute_vfs_save\n"
            )
            ring0_path = KERNEL_DIR / "context.py"
            ring0_tree = ast.parse(bad_ring0_code, filename=str(ring0_path))
            buf_ring0 = io.StringIO()
            with contextlib.redirect_stdout(buf_ring0):
                KernelFitnessVisitor(ring0_path, "context.py", is_test=False).visit(ring0_tree)
            out_ring0 = buf_ring0.getvalue()
            self.assertIn("KERNEL_RING_DAG_MANDATE", out_ring0)
            self.assertIn("Ring 0 module 'context' cannot import upward from Ring 4 module 'akasa.extension'", out_ring0)
            self.assertIn("Ring 0 module 'context' cannot import upward from Ring 2 module 'akasa.vfs'", out_ring0)

            # 2. Ring 2 (state.py) importing Flask transport or using an inline import
            bad_ring2_code = (
                "from flask import request\n"
                "def bad_helper():\n"
                "    import json as _json\n"
                "    return _json\n"
            )
            ring2_path = KERNEL_DIR / "state.py"
            ring2_tree = ast.parse(bad_ring2_code, filename=str(ring2_path))
            buf_ring2 = io.StringIO()
            with contextlib.redirect_stdout(buf_ring2):
                KernelFitnessVisitor(ring2_path, "state.py", is_test=False).visit(ring2_tree)
            out_ring2 = buf_ring2.getvalue()
            self.assertIn("KERNEL_TRANSPORT_ISOLATION", out_ring2)
            self.assertIn("STRICT_TOP_LEVEL_IMPORTS_MANDATE", out_ring2)
        finally:
            FitnessState.violations_found = prev


if __name__ == "__main__":
    unittest.main()