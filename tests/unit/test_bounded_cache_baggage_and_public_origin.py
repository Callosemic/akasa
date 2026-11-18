"""Behavioral tests for BoundedCache, ExecutionFrame baggage, public-origin read-only VFS, and MemoryVFSTransaction."""
import json
import os
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from flask import Flask

import akasa
import akasa.utils as akasa_utils
from akasa.auth import get_boot_token
from akasa.context import (
    BoundedCache,
    execution_frame,
    get_current_baggage,
    get_current_origin,
    get_registered_cache_stats,
)
from akasa.db import evict_cached_connection, get_connection, register_schema
from akasa.state import get_kernel_memory_telemetry
from akasa.vfs import (
    DefaultVFSDriver,
    MemoryVFSTransaction,
    VFSTransaction,
    execute_vfs_bulk,
    execute_vfs_delete,
    execute_vfs_move,
    execute_vfs_save,
    mount_volume,
    register_driver,
    sync_vfs_barrier,
)
from akasa.workers import (
    _execute_immediate_job,
    _execute_job,
    _init_worker_schema,
    _prepare_job_payload,
    register_callback,
    run_parallel,
)


class TestBoundedCache(unittest.TestCase):
    """BoundedCache enforces LRU capacity, revision matching, monotonic TTL, defensive cloning, and reset purging."""

    def setUp(self):
        akasa.reset_state()

    def tearDown(self):
        akasa.reset_state()

    def test_lru_eviction_revision_matching_and_cloning(self):
        cache = BoundedCache("unit_probe", max_entries=2, copy_mode="json")
        cache.put("a", {"items": [1, 2]}, revision=10)
        cache.put("b", {"items": [3]}, revision=20)

        # Accessing 'a' promotes it to MRU; inserting 'c' must evict 'b'.
        hit_a = cache.get("a", revision=10)
        self.assertEqual(hit_a, {"items": [1, 2]})
        hit_a["items"].append(99)
        self.assertEqual(cache.get("a", revision=10)["items"], [1, 2], "JSON clone must isolate caller mutations")

        cache.put("c", {"items": [4]}, revision=30)
        self.assertNotIn("b", cache)
        self.assertIn("a", cache)
        self.assertIn("c", cache)

        # Revision mismatch returns default without evicting newer entry
        self.assertIsNone(cache.get("a", revision=11))
        self.assertEqual(cache.get("a", revision=10), {"items": [1, 2]})

        # Telemetry & reset_state integration
        self.assertEqual(get_registered_cache_stats().get("cache_unit_probe_entries"), 2)
        self.assertEqual(get_kernel_memory_telemetry().get("cache_unit_probe_entries"), 2)
        self.assertGreater(cache.approx_kb(), 0.0)

        akasa.reset_state()
        self.assertEqual(len(cache), 0)

    def test_ttl_expiration_and_clear_where(self):
        cache = BoundedCache("unit_ttl", max_entries=8, ttl=0.05, copy_mode="none")
        cache.put("ephemeral", "val")
        self.assertIn("ephemeral", cache)
        time.sleep(0.08)
        self.assertNotIn("ephemeral", cache)
        self.assertIsNone(cache.get("ephemeral"))

        cache.put("keep_1", 1)
        cache.put("drop_1", 2)
        cache.put("drop_2", 3)
        removed = cache.clear_where(lambda k: str(k).startswith("drop_"))
        self.assertEqual(removed, 2)
        self.assertEqual(list(cache._store.keys()), ["keep_1"])


class TestExecutionFrameBaggageAndWorkers(unittest.TestCase):
    """ExecutionFrame baggage merges across nested frames, run_parallel(), and worker job payloads."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name).resolve()
        (self.root / ".akasa" / "cache" / "db").mkdir(parents=True, exist_ok=True)
        self.orig_cwd = akasa_utils.get_daemon_cwd()
        self.orig_ctrl = os.environ.get("AKASA_CONTROL_DIR")
        akasa.reset_state(cwd=self.root.as_posix(), control_dir=".akasa")
        (self.root / ".akasa" / "workspaces.json").write_text(
            json.dumps({"workspaces": {"default": {"config_path": "config.json"}}}), encoding="utf-8"
        )
        (self.root / ".akasa" / "config.json").write_text(
            json.dumps({"workspace_root": self.root.as_posix(), "extensions": ["baggage_ext"]}), encoding="utf-8"
        )
        _init_worker_schema("default")

    def tearDown(self):
        akasa.reset_state(cwd=self.orig_cwd, control_dir=self.orig_ctrl or ".akasa")
        self.temp_dir.cleanup()

    def test_nested_baggage_inheritance_and_run_parallel_propagation(self):
        self.assertEqual(get_current_baggage(), {})
        with execution_frame(workspace_id="default", transport="HTTP", origin="mcp", baggage={"trace_id": "t-1", "empty": ""}):
            snap = get_current_baggage()
            self.assertEqual(snap, {"trace_id": "t-1"})
            snap["trace_id"] = "mutated"
            self.assertEqual(get_current_baggage(), {"trace_id": "t-1"})

            with execution_frame(transport="IPC", baggage={"step": "s-2", "ignored": None}):
                self.assertEqual(get_current_baggage(), {"trace_id": "t-1", "step": "s-2"})
                observed = run_parallel(
                    lambda item: (item, get_current_origin(), get_current_baggage()),
                    ["x", "y"],
                    max_workers=2,
                )
                self.assertEqual(
                    observed,
                    [
                        ("x", "mcp", {"trace_id": "t-1", "step": "s-2"}),
                        ("y", "mcp", {"trace_id": "t-1", "step": "s-2"}),
                    ],
                )
            self.assertEqual(get_current_baggage(), {"trace_id": "t-1"})

    def test_worker_jobs_inherit_submitter_baggage_and_origin(self):
        seen_imm = []
        seen_sched = []
        register_callback("baggage_ext", "imm_task", lambda **kw: seen_imm.append((get_current_origin(), get_current_baggage())))
        register_callback("baggage_ext", "sched_task", lambda **kw: seen_sched.append((get_current_origin(), get_current_baggage())))

        with execution_frame(workspace_id="default", transport="HTTP", origin="agent", baggage={"run_id": "r-9"}):
            _ws, conn, now, args_json = _prepare_job_payload({"x": 1}, "default")

        parsed = json.loads(args_json)
        self.assertEqual(parsed["_origin"], "agent")
        self.assertEqual(parsed["_baggage"], {"run_id": "r-9"})

        conn.execute(
            "INSERT OR REPLACE INTO immediate_jobs (id, ext_name, callback_name, status, args_json, created_at, updated_at) "
            "VALUES ('imm_bag_1', 'baggage_ext', 'imm_task', 'processing', ?, ?, ?)",
            (args_json, now, now),
        )
        conn.commit()
        _execute_immediate_job("imm_bag_1", "baggage_ext", "imm_task", args_json, "default")
        self.assertEqual(seen_imm, [("agent", {"run_id": "r-9"})])

        # _execute_immediate_job closes this thread's pooled connections when it finishes.
        conn = get_connection("workers", workspace_id="default")
        conn.execute(
            "INSERT OR REPLACE INTO jobs (id, ext_name, callback_name, interval_ms, status, args_json) "
            "VALUES ('sched_bag_1', 'baggage_ext', 'sched_task', 0, 'running', ?)",
            (args_json,),
        )
        conn.commit()
        _execute_job("sched_bag_1", "baggage_ext", "sched_task", 0, 0, args_json, "default")
        self.assertEqual(seen_sched, [("agent", {"run_id": "r-9"})])


class TestPublicOriginReadOnlyVfsAndMemoryVfs(unittest.TestCase):
    """Unauthenticated public routes execute with origin='public' and are blocked from mutating the VFS."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name).resolve()
        self.control = self.root / ".akasa"
        (self.control / "cache" / "db").mkdir(parents=True, exist_ok=True)
        self.repo = self.root / "repo"
        self.repo.mkdir()
        self.orig_cwd = akasa_utils.get_daemon_cwd()
        self.orig_ctrl = os.environ.get("AKASA_CONTROL_DIR")
        akasa.reset_state(cwd=self.root.as_posix(), control_dir=".akasa")
        (self.control / "workspaces.json").write_text(
            json.dumps({"workspaces": {"default": {"config_path": "config.json"}}}), encoding="utf-8"
        )
        (self.control / "config.json").write_text(
            json.dumps({"workspace_root": self.root.as_posix()}), encoding="utf-8"
        )
        _init_worker_schema("default")
        register_driver("vfs", DefaultVFSDriver())
        mount_volume("default", "vfs", "repo", self.repo.as_posix())

    def tearDown(self):
        akasa.reset_state(cwd=self.orig_cwd, control_dir=self.orig_ctrl or ".akasa")
        self.temp_dir.cleanup()

    def test_public_route_frame_blocks_vfs_mutations_until_authenticated(self):
        app = Flask(f"pub_ro_{id(self)}")
        akasa.init_app(app, token_header="X-Test-Token", public_routes={"/share/view"}, public_prefixes=("/pub/",))

        @app.route("/share/view", methods=["POST"])
        def share_view():
            origin = get_current_origin()
            save_res = execute_vfs_save("default", "vfs://repo/unauth.txt", "blocked")
            _, del_status = execute_vfs_delete("default", "vfs://repo/unauth.txt")
            _, mov_status = execute_vfs_move("default", "vfs://repo/a.txt", "vfs://repo/b.txt")
            _, blk_status = execute_vfs_bulk("default", [{"action": "save", "filepath": "vfs://repo/c.txt", "content": "x"}])
            return {
                "origin": origin,
                "save_status": save_res["status"],
                "del_status": del_status,
                "mov_status": mov_status,
                "blk_status": blk_status,
            }

        client = app.test_client()
        # 1. Unauthenticated request enters origin='public' and every VFS write is rejected
        unauth = client.post("/share/view", headers={"Host": "localhost"}).get_json()
        self.assertEqual(unauth["origin"], "public")
        self.assertEqual(unauth["save_status"], "error")
        self.assertEqual((unauth["del_status"], unauth["mov_status"], unauth["blk_status"]), (403, 403, 403))
        self.assertIsNone(get_current_origin(), "Public frame must exit cleanly on teardown_request")

        # 2. Authenticated request to the same public route is NOT downgraded to 'public'
        token = get_boot_token()
        auth = client.post("/share/view", headers={"Host": "localhost", "X-Test-Token": token}).get_json()
        self.assertIsNone(auth["origin"])
        self.assertEqual(auth["save_status"], "accepted")
        self.assertEqual((auth["del_status"], auth["blk_status"]), (202, 202))
        sync_vfs_barrier("default")
        # The route saves unauth.txt and then deletes it, so the bulk save of c.txt is the write that must land.
        self.assertTrue((self.repo / "c.txt").exists())
        self.assertFalse((self.repo / "unauth.txt").exists())

    def test_public_origin_is_sticky_under_nested_explicit_origin(self):
        with execution_frame(workspace_id="default", transport="HTTP", origin="public"):
            with execution_frame(transport="IPC", origin="agent"):
                self.assertEqual(get_current_origin(), "public")
                self.assertEqual(execute_vfs_save("default", "vfs://repo/escalated.txt", "x")["status"], "error")
                _, del_status = execute_vfs_delete("default", "vfs://repo/escalated.txt")
                self.assertEqual(del_status, 403)
        sync_vfs_barrier("default")
        self.assertFalse((self.repo / "escalated.txt").exists())

    def test_memory_vfs_transaction_and_read_with_revision(self):
        mem = MemoryVFSTransaction({"vfs://repo/init.md": "hello"})
        rev1, txt1 = mem.read_with_revision("vfs://repo/init.md")
        self.assertIsNotNone(rev1)
        self.assertEqual(txt1, "hello")

        with self.assertRaises(ValueError):
            mem.read("relative/without_scheme.md")

        mem.save("vfs://repo/init.md", "updated")
        rev2, txt2 = mem.read_with_revision("vfs://repo/init.md")
        self.assertNotEqual(rev1, rev2)
        self.assertEqual(txt2, "updated")

        mem.move("vfs://repo/init.md", "vfs://repo/moved.md")
        self.assertIsNone(mem.read("vfs://repo/init.md"))
        self.assertEqual(list(mem.walk("vfs://repo", exts=[".md"])), ["vfs://repo/moved.md"])

        mem.bulk([
            {"action": "save", "filepath": "vfs://repo/extra.txt", "content": "1"},
            {"action": "delete", "filepath": "vfs://repo/moved.md"},
        ])
        self.assertEqual(mem.files, {"vfs://repo/extra.txt": "1"})

        # Disk VFSTransaction read_with_revision
        vfs = VFSTransaction("default")
        vfs.save("vfs://repo/disk.txt", "v1")
        d_rev, d_txt = vfs.read_with_revision("vfs://repo/disk.txt")
        self.assertIsNotNone(d_rev)
        self.assertEqual(d_txt, "v1")

    def test_evicted_connection_reapplies_schema_on_reopen(self):
        register_schema("evict_probe", {"items": {"id": "TEXT PRIMARY KEY"}})
        conn1 = get_connection("evict_probe", workspace_id="default")
        conn1.execute("INSERT INTO items (id) VALUES ('k1')")
        conn1.commit()
        evict_cached_connection("default", "evict_probe")
        conn2 = get_connection("evict_probe", workspace_id="default")
        row = conn2.execute("SELECT id FROM items WHERE id='k1'").fetchone()
        self.assertEqual(row["id"], "k1")

    def test_reset_retires_connections_cached_on_other_threads(self):
        with ThreadPoolExecutor(max_workers=1) as pool:
            first = pool.submit(get_connection, "gen_probe", "default").result()
            self.assertIs(pool.submit(get_connection, "gen_probe", "default").result(), first)
            akasa.reset_state(cwd=self.root.as_posix(), control_dir=".akasa")
            second = pool.submit(get_connection, "gen_probe", "default").result()
        self.assertIsNot(second, first)
        with self.assertRaisesRegex(Exception, "closed database"):
            first.execute("SELECT 1")


if __name__ == "__main__":
    unittest.main()
