import json
import os
import tempfile
import time
import unittest
import uuid
from pathlib import Path
from typing import Dict, List, Literal, Optional, Tuple, Union
from unittest.mock import patch

import akasa
import akasa.utils as akasa_utils
from akasa.db import get_connection
from akasa.extension import AkasaExtension
from akasa.workers import (
    _execute_immediate_job,
    _execute_job,
    _init_worker_schema,
    register_callback,
    validate_dag_chain_kwargs,
)


class TestAkasaWorkerContracts(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root_path = Path(self.temp_dir.name).resolve()
        self.control_dir = self.root_path / ".akasa"
        (self.control_dir / "cache" / "db").mkdir(parents=True, exist_ok=True)

        self.orig_cwd = akasa_utils.get_daemon_cwd()
        self.orig_env_ctrl = os.environ.get("AKASA_CONTROL_DIR")
        akasa.reset_state(cwd=self.root_path.as_posix(), control_dir=".akasa")

        self.workspace_id = f"ws_{uuid.uuid4().hex[:8]}"
        (self.control_dir / "workspaces.json").write_text(
            json.dumps({
                "workspaces": {
                    "default": {"config_path": "config.json"},
                    self.workspace_id: {"config_path": (self.control_dir / "config.json").as_posix()},
                }
            }),
            encoding="utf-8",
        )
        (self.control_dir / "config.json").write_text(
            json.dumps({"workspace_root": self.root_path.as_posix(), "extensions": ["contract_test"]}),
            encoding="utf-8",
        )
        _init_worker_schema(self.workspace_id)

    def tearDown(self):
        akasa.reset_state(cwd=self.orig_cwd, control_dir=self.orig_env_ctrl or ".akasa")
        try:
            self.temp_dir.cleanup()
        except OSError:
            pass

    def test_signature_reflection_and_synchronous_type_validation(self):
        """AkasaExtension.worker reflects parameter types and synchronously rejects invalid types, unknown kwargs, and non-JSON objects."""
        contract_bp = AkasaExtension("contract_test", __name__, core=True)
        captured_calls = []
        @contract_bp.worker("strict_task")
        def _strict_worker(
            ctx,
            force_full: bool = False,
            retry_count: int = 0,
            mode: Literal["fast", "deep"] = "fast",
            target_repos: Optional[List[str]] = None,
            metadata: Optional[Dict[str, int]] = None,
            tags: Optional[Tuple[str, ...]] = None,
            pair: Optional[Tuple[str, int]] = None,
        ):
            captured_calls.append({
                "force_full": force_full,
                "retry_count": retry_count,
                "mode": mode,
                "target_repos": target_repos,
                "metadata": metadata,
                "current_job_id": ctx.jobs.current_job_id,
                "root_job_id": ctx.jobs.root_job_id,
                "chain_state": ctx.jobs.chain_state,
                "chain_history": ctx.jobs.chain_history,
            })
            return {
                "message": "Strict worker executed.",
                "artifact": {"state_deltas": {"touched_buckets": ["alpha::main"]}},
            }

        ctx = contract_bp.get_context(self.workspace_id)

        # 1. Passing list[str] to force_full: bool raises TypeError synchronously
        with self.assertRaises(TypeError) as err_list_for_bool:
            ctx.jobs.submit("strict_task", job_category="system_background", force_full=["alpha"])
        self.assertIn("force_full", str(err_list_for_bool.exception))
        self.assertIn("expected bool, got list", str(err_list_for_bool.exception))

        # 2. Passing bool to int parameter (isinstance(True, int) guard) raises TypeError
        with self.assertRaises(TypeError) as err_bool_for_int:
            ctx.jobs.submit("strict_task", job_category="system_background", retry_count=True)
        self.assertIn("retry_count", str(err_bool_for_int.exception))
        self.assertIn("expected int, got bool", str(err_bool_for_int.exception))

        # 3. Invalid Literal value raises TypeError
        with self.assertRaises(TypeError) as err_bad_literal:
            ctx.jobs.submit("strict_task", job_category="system_background", mode="invalid_mode")
        self.assertIn("mode", str(err_bad_literal.exception))

        # 4. Wrong list element type raises TypeError
        with self.assertRaises(TypeError) as err_bad_list:
            ctx.jobs.submit("strict_task", job_category="system_background", target_repos=[123])
        self.assertIn("target_repos", str(err_bad_list.exception))
        # 5. Wrong dict value type raises TypeError
        with self.assertRaises(TypeError) as err_bad_dict:
            ctx.jobs.submit("strict_task", job_category="system_background", metadata={"count": "not_an_int"})
        self.assertIn("metadata", str(err_bad_dict.exception))

        # 5b. Variable-length Tuple[str, ...] and fixed-length Tuple[str, int] validation
        with self.assertRaises(TypeError) as err_bad_var_tuple:
            ctx.jobs.submit("strict_task", job_category="system_background", tags=("valid_str", 999))
        self.assertIn("tags", str(err_bad_var_tuple.exception))

        with self.assertRaises(TypeError) as err_bad_fixed_tuple:
            ctx.jobs.submit("strict_task", job_category="system_background", pair=("label", "not_an_int"))
        self.assertIn("pair", str(err_bad_fixed_tuple.exception))

        # 6. Unknown keyword argument raises TypeError
        with self.assertRaises(TypeError) as err_unknown:
            ctx.jobs.submit("strict_task", job_category="system_background", typo_arg="val")
        self.assertIn("unexpected keyword argument", str(err_unknown.exception))
        self.assertIn("typo_arg", str(err_unknown.exception))

        # 7. Non-JSON-serializable argument raises TypeError
        with self.assertRaises(TypeError) as err_json:
            ctx.jobs.submit("strict_task", job_category="system_background", target_repos=object())
        self.assertIn("JSON-serializable", str(err_json.exception))

        # 8. Envelope isolation on ctx.jobs during _execute_immediate_job
        conn = get_connection("workers", workspace_id=self.workspace_id)
        now = time.time()
        envelope_payload = {
            "force_full": True,
            "retry_count": 2,
            "mode": "deep",
            "target_repos": ["alpha"],
            "metadata": {"files": 5},
            "root_job_id": "chn_root123",
            "chain_state": {"upstream_flag": "ok"},
            "chain_history": [{"ext_name": "topology", "job_id": "tpl_1", "status": "completed"}],
        }
        conn.execute(
            "INSERT OR REPLACE INTO immediate_jobs (id, ext_name, callback_name, status, args_json, created_at, updated_at, job_category) "
            "VALUES (?, ?, ?, 'processing', ?, ?, ?, 'ui_blocking')",
            ("job_strict_1", "contract_test", "strict_task", json.dumps(envelope_payload), now, now),
        )
        conn.commit()

        # Verify IoC predicate on find_active_pipeline_job while job is in 'processing' status
        matched_id = ctx.jobs.find_active_pipeline_job(
            ["strict_task"],
            predicate=lambda args: args.get("force_full") is True and args.get("retry_count") == 2,
        )
        self.assertEqual(matched_id, "job_strict_1")
        unmatched_id = ctx.jobs.find_active_pipeline_job(
            ["strict_task"],
            predicate=lambda args: args.get("retry_count") == 99,
        )
        self.assertIsNone(unmatched_id)

        _execute_immediate_job(
            "job_strict_1",
            "contract_test",
            "strict_task",
            json.dumps(envelope_payload),
            self.workspace_id,
        )

        self.assertEqual(len(captured_calls), 1)
        call_data = captured_calls[0]
        self.assertTrue(call_data["force_full"])
        self.assertEqual(call_data["retry_count"], 2)
        self.assertEqual(call_data["mode"], "deep")
        self.assertEqual(call_data["target_repos"], ["alpha"])
        self.assertEqual(call_data["metadata"], {"files": 5})
        self.assertEqual(call_data["current_job_id"], "job_strict_1")
        self.assertEqual(call_data["root_job_id"], "chn_root123")
        self.assertEqual(call_data["chain_state"], {"upstream_flag": "ok"})
        self.assertEqual(len(call_data["chain_history"]), 1)

    def test_dag_chain_cross_step_validation(self):
        """validate_dag_chain_kwargs allows step-specific kwargs across a chain but rejects unknown kwargs not accepted by any step."""
        step_a_bp = AkasaExtension("step_a", __name__, core=True)
        step_b_bp = AkasaExtension("step_b", __name__, core=True)

        @step_a_bp.worker("scan")
        def _scan(ctx, force_full: bool = False):
            return "scanned"

        @step_b_bp.worker("compile")
        def _compile(ctx, force_full: bool = False, target_repos: Optional[List[str]] = None):
            return "compiled"

        steps = [
            {"id": "a", "ext_name": "step_a", "worker_name": "scan"},
            {"id": "b", "ext_name": "step_b", "worker_name": "compile", "depends_on": ["a"]},
        ]

        # Valid: target_repos is accepted by step_b even though step_a does not declare it
        validate_dag_chain_kwargs(steps, {"force_full": True, "target_repos": ["repo_1"]})

        # Invalid type on shared parameter raises TypeError
        with self.assertRaises(TypeError):
            validate_dag_chain_kwargs(steps, {"force_full": ["not_a_bool"]})

        # Unknown kwarg not accepted by ANY step in the DAG raises TypeError
        with self.assertRaises(TypeError) as err_dag_unknown:
            validate_dag_chain_kwargs(steps, {"force_full": False, "completely_bogus": 123})
        self.assertIn("completely_bogus", str(err_dag_unknown.exception))

    @patch("akasa.workers.is_extension_enabled")
    def test_worker_drops_recurring_job_for_disabled_extension(self, mock_is_enabled):
        """_execute_job drops and deletes pending recurring jobs if extension is disabled."""
        mock_is_enabled.return_value = False
        executed = False

        def sample_callback(**kwargs):
            nonlocal executed
            executed = True

        register_callback("custom_ext", "sample_task", sample_callback)

        conn = get_connection("workers", workspace_id=self.workspace_id)
        conn.execute(
            "INSERT OR REPLACE INTO jobs (id, ext_name, callback_name, interval_ms, status) VALUES (?, ?, ?, ?, 'running')",
            ("job_recurring_123", "custom_ext", "sample_task", 60000),
        )
        conn.commit()
        _execute_job(
            job_id="job_recurring_123",
            ext_name="custom_ext",
            callback_name="sample_task",
            interval_ms=60000,
            jitter_ms=0,
            args_json="{}",
            fallback_ws=self.workspace_id,
        )

        self.assertFalse(executed, "Callback must not execute for disabled extensions.")
        fresh_conn = get_connection("workers", workspace_id=self.workspace_id)
        row = fresh_conn.execute("SELECT id FROM jobs WHERE id=?", ("job_recurring_123",)).fetchone()
        self.assertIsNone(row, "Job record must be purged from jobs table.")

    @patch("akasa.workers.is_extension_enabled")
    def test_immediate_job_fails_safely_for_disabled_extension(self, mock_is_enabled):
        """_execute_immediate_job logs failure without executing callback if extension is disabled."""
        mock_is_enabled.return_value = False
        executed = False

        def immediate_callback(**kwargs):
            nonlocal executed
            executed = True

        register_callback("disabled_ext", "immediate_task", immediate_callback)
        conn = get_connection("workers", workspace_id=self.workspace_id)
        now = time.time()
        conn.execute(
            "INSERT OR REPLACE INTO immediate_jobs (id, ext_name, callback_name, status, created_at, updated_at) VALUES (?, ?, ?, 'processing', ?, ?)",
            ("imm_456", "disabled_ext", "immediate_task", now, now),
        )
        conn.commit()

        _execute_immediate_job(
            job_id="imm_456",
            ext_name="disabled_ext",
            callback_name="immediate_task",
            args_json="{}",
            target_ws=self.workspace_id,
        )

        self.assertFalse(executed, "Immediate callback must not execute for disabled extensions.")
        fresh_conn = get_connection("workers", workspace_id=self.workspace_id)
        row = fresh_conn.execute("SELECT status, status_message FROM immediate_jobs WHERE id=?", ("imm_456",)).fetchone()
        self.assertEqual(row["status"], "failed")
        self.assertIn("disabled", row["status_message"].lower())


if __name__ == "__main__":
    unittest.main()