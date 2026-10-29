import ast
import contextlib
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from flask import Flask

import akasa
import akasa.context as akasa_context
import akasa.utils as akasa_utils
from akasa.auth.peer import (
    classify_peer,
    is_host_allowed,
    is_loopback_origin,
    normalize_hostname,
    register_allowed_host,
    register_proxy_header,
    get_proxy_headers,
)
from akasa.hooks import hooks
from tests.fitness.core import FitnessState, KERNEL_DIR
from tests.fitness.rules_python import KernelFitnessVisitor


class TestPeerClassification(unittest.TestCase):
    """S1: classify_peer is the single source of truth for request origin and fails closed on any proxy signal."""

    def setUp(self):
        akasa.reset_state()

    def tearDown(self):
        akasa.reset_state()

    def test_direct_loopback_without_proxy_headers(self):
        peer = classify_peer("127.0.0.1", {"Host": "127.0.0.1:5005"})
        self.assertTrue(peer.is_direct_loopback)
        self.assertFalse(peer.via_proxy)
        self.assertEqual(peer.client_ip, "127.0.0.1")
        self.assertEqual(peer.host, "127.0.0.1")

    def test_ipv6_and_ipv4_mapped_loopback_peers(self):
        self.assertTrue(classify_peer("::1", {"Host": "[::1]:5005"}).is_direct_loopback)
        self.assertTrue(classify_peer("::ffff:127.0.0.1", {"Host": "localhost"}).is_direct_loopback)
        self.assertTrue(classify_peer("127.0.0.2", {"Host": "localhost"}).is_direct_loopback)

    def test_spoofed_leftmost_forwarded_for_never_grants_loopback(self):
        """An appending proxy yields 'spoof, real'; the rightmost hop is trusted and the request is never local."""
        peer = classify_peer("127.0.0.1", {"Host": "127.0.0.1:5005", "X-Forwarded-For": "127.0.0.1, 100.76.11.32"})
        self.assertTrue(peer.via_proxy)
        self.assertFalse(peer.is_direct_loopback)
        self.assertEqual(peer.client_ip, "100.76.11.32")

    def test_single_value_forwarded_for_still_disqualifies_loopback(self):
        """Even a forwarding header claiming 127.0.0.1 marks the request as proxied."""
        peer = classify_peer("127.0.0.1", {"Host": "localhost", "X-Forwarded-For": "127.0.0.1"})
        self.assertTrue(peer.via_proxy)
        self.assertFalse(peer.is_direct_loopback)

    def test_any_forwarding_header_marks_request_proxied(self):
        for header in ("Forwarded", "X-Forwarded-Host", "X-Forwarded-Proto", "X-Forwarded-Port", "X-Real-IP"):
            with self.subTest(header=header):
                peer = classify_peer("127.0.0.1", {"Host": "localhost", header: "x"})
                self.assertTrue(peer.via_proxy)
                self.assertFalse(peer.is_direct_loopback)

    def test_registered_proxy_header_marks_request_proxied(self):
        self.assertFalse(classify_peer("127.0.0.1", {"Host": "localhost", "X-Example-Identity": "a"}).via_proxy)
        register_proxy_header("X-Example-Identity")
        self.assertIn("x-example-identity", get_proxy_headers())
        peer = classify_peer("127.0.0.1", {"Host": "localhost", "x-example-identity": "a"})
        self.assertTrue(peer.via_proxy)
        self.assertFalse(peer.is_direct_loopback)

    def test_reset_state_clears_registered_proxy_headers(self):
        register_proxy_header("X-Example-Identity")
        akasa.reset_state()
        self.assertNotIn("x-example-identity", get_proxy_headers())

    def test_forwarded_for_from_non_loopback_peer_is_ignored(self):
        """Only a proxy on this machine may report the client address."""
        peer = classify_peer("192.168.1.50", {"Host": "localhost", "X-Forwarded-For": "100.76.11.32"})
        self.assertTrue(peer.via_proxy)
        self.assertEqual(peer.client_ip, "192.168.1.50")
        self.assertFalse(peer.is_direct_loopback)

    def test_non_loopback_peer_without_headers_is_not_local(self):
        peer = classify_peer("192.168.1.50", {"Host": "localhost"})
        self.assertFalse(peer.via_proxy)
        self.assertFalse(peer.is_direct_loopback)
        self.assertEqual(peer.client_ip, "192.168.1.50")

    def test_real_ip_fallback_and_malformed_forwarded_for(self):
        peer = classify_peer("127.0.0.1", {"Host": "localhost", "X-Real-IP": "100.76.11.32"})
        self.assertEqual(peer.client_ip, "100.76.11.32")
        peer_bad = classify_peer("127.0.0.1", {"Host": "localhost", "X-Forwarded-For": "1.2.3.4, not-an-ip"})
        self.assertEqual(peer_bad.client_ip, "")
        self.assertFalse(peer_bad.is_direct_loopback)

    def test_rightmost_forwarded_ip_tolerates_ports_and_brackets(self):
        self.assertEqual(classify_peer("127.0.0.1", {"X-Forwarded-For": "9.9.9.9, 100.76.11.32:41641"}).client_ip, "100.76.11.32")
        self.assertEqual(classify_peer("127.0.0.1", {"X-Forwarded-For": "[fd7a:115c:a1e0::1]:443"}).client_ip, "fd7a:115c:a1e0::1")

    def test_header_lookup_is_case_insensitive_for_plain_dicts(self):
        peer = classify_peer("127.0.0.1", {"host": "LOCALHOST.", "x-FORWARDED-for": "100.76.11.32"})
        self.assertTrue(peer.via_proxy)
        self.assertEqual(peer.host, "localhost")
        self.assertEqual(peer.client_ip, "100.76.11.32")

    def test_missing_or_garbage_inputs_never_raise(self):
        peer = classify_peer(None, None)
        self.assertFalse(peer.is_direct_loopback)
        self.assertEqual(peer.client_ip, "")
        self.assertFalse(classify_peer("not-an-ip", {"Host": "localhost"}).is_direct_loopback)

    def test_hostname_normalization(self):
        self.assertEqual(normalize_hostname("[::1]:5005"), "::1")
        self.assertEqual(normalize_hostname("LocalHost.:80"), "localhost")
        self.assertEqual(normalize_hostname("::1"), "::1")
        self.assertEqual(normalize_hostname(""), "")

    def test_host_allowlist_respects_suffix_boundaries(self):
        suffixes = (".example.net",)
        self.assertTrue(is_host_allowed("box.example.net:9005", suffixes))
        self.assertFalse(is_host_allowed("evilexample.net", suffixes))
        self.assertFalse(is_host_allowed("example.net", suffixes), "A bare suffix is not a host under it")
        self.assertTrue(is_host_allowed("box.example.net", ("example.net",)), "Suffixes registered without a dot still match on a label boundary")
        self.assertFalse(is_host_allowed("evil.example", suffixes))
        self.assertFalse(is_host_allowed("", suffixes))
        self.assertTrue(is_host_allowed("[::1]:5005", ()))

    def test_registered_allowed_hosts_match_exactly(self):
        self.assertFalse(is_host_allowed("dev.example.org", ()))
        register_allowed_host("Dev.Example.org:443")
        self.assertTrue(is_host_allowed("dev.example.org", ()))
        self.assertFalse(is_host_allowed("sub.dev.example.org", ()))

    def test_loopback_origin_detection(self):
        self.assertTrue(is_loopback_origin("http://localhost:3000"))
        self.assertTrue(is_loopback_origin("http://127.0.0.1:5005/app?x=1"))
        self.assertTrue(is_loopback_origin("http://[::1]:5005"))
        self.assertFalse(is_loopback_origin("null"))
        self.assertFalse(is_loopback_origin("https://box.example.net"))
        self.assertFalse(is_loopback_origin("http://127.0.0.1.evil.example"))
        self.assertFalse(is_loopback_origin("http://localhost@evil.example"))


class TestBootstrapAndHostGate(unittest.TestCase):
    """S1: /auth/bootstrap only grants localhost trust to direct loopback requests; the gate rejects foreign Hosts."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root_path = Path(self.temp_dir.name).resolve()
        self.control_dir = self.root_path / ".akasa"
        (self.control_dir / "cache" / "db").mkdir(parents=True, exist_ok=True)
        self.orig_cwd = akasa_utils.get_daemon_cwd()
        self.orig_env_ctrl = os.environ.get("AKASA_CONTROL_DIR")
        akasa.reset_state(cwd=self.root_path.as_posix(), control_dir=".akasa")
        (self.control_dir / "workspaces.json").write_text(
            json.dumps({"workspaces": {"default": {"config_path": "config.json"}}}),
            encoding="utf-8",
        )
        (self.control_dir / "config.json").write_text(
            json.dumps({"workspace_root": self.root_path.as_posix(), "auth_token": "static-secret"}),
            encoding="utf-8",
        )
        self.suffix_patch = patch.object(akasa_context, "_TRUSTED_HOST_SUFFIXES", {".example.net"})
        self.suffix_patch.start()
        self.handshake_calls = []
        self.app = self._build_app()
        self.client = self.app.test_client()

    def tearDown(self):
        self.suffix_patch.stop()
        handlers = hooks._hooks.get("execute_identity_handshake", [])
        hooks._hooks["execute_identity_handshake"] = [
            item for item in handlers if getattr(item[1], "__name__", "") != "_record_handshake"
        ]
        akasa.reset_state(cwd=self.orig_cwd, control_dir=self.orig_env_ctrl or ".akasa")
        try:
            self.temp_dir.cleanup()
        except OSError:
            pass

    def _build_app(self, **init_kwargs):
        app = Flask(f"auth_peer_trust_{id(self)}_{len(init_kwargs)}")
        akasa.init_app(app, token_header="X-Test-Token", public_routes={"/public"}, **init_kwargs)

        @app.route("/public")
        def public_route():
            return "public"

        @app.route("/private")
        def private_route():
            return "private"

        return app

    def _register_recording_provider(self, result=None):
        calls = self.handshake_calls

        @hooks.on("execute_identity_handshake")
        def _record_handshake(request=None, peer=None, client_ip=None, config=None, **kwargs):
            calls.append({"peer": peer, "client_ip": client_ip})
            return result

    def _bootstrap(self, remote_addr="127.0.0.1", headers=None, body=None):
        return self.client.post(
            "/auth/bootstrap",
            json=body if body is not None else {},
            headers=headers or {},
            environ_overrides={"REMOTE_ADDR": remote_addr},
        )

    def test_direct_loopback_bootstrap_still_authenticates(self):
        """CLI health checks and local browsers keep the localhost route."""
        res = self._bootstrap(headers={"Host": "127.0.0.1:5005"})
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.get_json()["method"], "localhost")

    def test_spoofed_forwarded_for_is_denied_localhost_trust(self):
        res = self._bootstrap(headers={"Host": "127.0.0.1:5005", "X-Forwarded-For": "127.0.0.1"})
        self.assertEqual(res.status_code, 401)
        self.assertEqual(res.get_json()["status"], "challenge")

    def test_appended_spoof_passes_rightmost_hop_to_identity_providers(self):
        self._register_recording_provider(result=None)
        res = self._bootstrap(headers={"Host": "box.example.net", "X-Forwarded-For": "127.0.0.1, 100.76.11.32"})
        self.assertEqual(res.status_code, 401)
        self.assertEqual(len(self.handshake_calls), 1)
        call = self.handshake_calls[0]
        self.assertEqual(call["client_ip"], "100.76.11.32")
        self.assertTrue(call["peer"].via_proxy)
        self.assertFalse(call["peer"].is_direct_loopback)

    def test_identity_provider_can_still_authenticate_proxied_requests(self):
        self._register_recording_provider(result={"status": "authenticated", "method": "provider", "user": "dev@example.org"})
        res = self._bootstrap(headers={"Host": "box.example.net", "X-Forwarded-For": "100.76.11.32"})
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.get_json()["method"], "provider")
        self.assertEqual(res.get_json()["user"], "dev@example.org")

    def test_static_token_route_works_behind_a_proxy(self):
        res = self._bootstrap(headers={"Host": "box.example.net", "X-Forwarded-For": "100.76.11.32"}, body={"token": "static-secret"})
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.get_json()["method"], "static_config")
        wrong = self._bootstrap(headers={"Host": "box.example.net", "X-Forwarded-For": "100.76.11.32"}, body={"token": "nope"})
        self.assertEqual(wrong.status_code, 401)

    def test_non_loopback_peer_is_never_localhost(self):
        res = self._bootstrap(remote_addr="192.168.1.50", headers={"Host": "localhost"})
        self.assertEqual(res.status_code, 401)

    def test_loopback_peer_with_non_loopback_host_is_not_localhost(self):
        res = self._bootstrap(headers={"Host": "box.example.net"})
        self.assertEqual(res.status_code, 401)

    def test_cross_site_origin_blocked_on_localhost_route(self):
        """Remote origins, including hosts under a trusted suffix, no longer qualify for the localhost route."""
        for origin in ("https://box.example.net", "https://evil.example", "null"):
            with self.subTest(origin=origin):
                res = self._bootstrap(headers={"Host": "localhost:5005", "Origin": origin})
                self.assertEqual(res.status_code, 403)
        ok = self._bootstrap(headers={"Host": "localhost:5005", "Origin": "http://localhost:5005"})
        self.assertEqual(ok.status_code, 200)
        bad_referer = self._bootstrap(headers={"Host": "localhost:5005", "Referer": "https://evil.example/page"})
        self.assertEqual(bad_referer.status_code, 403)

    def test_host_gate_rejects_foreign_hosts_on_every_route(self):
        for route in ("/public", "/private", "/auth/bootstrap"):
            with self.subTest(route=route):
                method = self.client.post if route == "/auth/bootstrap" else self.client.get
                kwargs = {"json": {}} if route == "/auth/bootstrap" else {}
                res = method(route, headers={"Host": "rebind.evil.example"}, **kwargs)
                self.assertEqual(res.status_code, 421)

    def test_host_gate_accepts_loopback_trusted_suffix_and_registered_hosts(self):
        self.assertEqual(self.client.get("/public", headers={"Host": "localhost:5005"}).status_code, 200)
        self.assertEqual(self.client.get("/public", headers={"Host": "[::1]:5005"}).status_code, 200)
        self.assertEqual(self.client.get("/public", headers={"Host": "box.example.net:9005"}).status_code, 200)
        self.assertEqual(self.client.get("/public", headers={"Host": "dev.example.org"}).status_code, 421)
        register_allowed_host("dev.example.org")
        self.assertEqual(self.client.get("/public", headers={"Host": "dev.example.org"}).status_code, 200)

    def test_host_gate_runs_before_token_check_and_token_still_required(self):
        token = self._bootstrap(headers={"Host": "localhost"}).get_json()["token"]
        self.assertEqual(self.client.get("/private", headers={"Host": "localhost"}).status_code, 401)
        self.assertEqual(self.client.get("/private", headers={"Host": "localhost", "X-Test-Token": token}).status_code, 200)
        self.assertEqual(self.client.get("/private", headers={"Host": "rebind.evil.example", "X-Test-Token": token}).status_code, 421)

    def test_host_validation_can_be_disabled_for_embedding(self):
        open_app = self._build_app(enforce_host_validation=False)
        res = open_app.test_client().get("/public", headers={"Host": "rebind.evil.example"})
        self.assertEqual(res.status_code, 200)


class TestForwardedHeaderMembraneRule(unittest.TestCase):
    """S1: the FORWARDED_HEADER_MEMBRANE fitness rule keeps forwarding-header reads inside akasa/auth/peer.py."""

    SNIPPET = (
        "def sniff(request, environ):\n"
        "    a = request.headers.get('X-Forwarded-For')\n"
        "    b = request.headers.get('X-Real-IP')\n"
        "    c = environ.get('HTTP_X_FORWARDED_FOR')\n"
        "    d = request.headers.get('X-Forwarded-Proto')\n"
    )

    def _violations(self, filepath, is_test=False):
        prev = FitnessState.violations_found
        FitnessState.violations_found = 0
        buf = io.StringIO()
        try:
            with contextlib.redirect_stdout(buf):
                KernelFitnessVisitor(filepath, Path(filepath).name, is_test=is_test).visit(ast.parse(self.SNIPPET))
            return FitnessState.violations_found, buf.getvalue()
        finally:
            FitnessState.violations_found = prev

    def test_reads_outside_peer_module_are_flagged(self):
        count, out = self._violations(KERNEL_DIR / "auth" / "__init__.py")
        self.assertEqual(out.count("FORWARDED_HEADER_MEMBRANE"), 3, out)
        self.assertNotIn("X-Forwarded-Proto", out, "Scheme headers are not client identity and stay allowed")

    def test_peer_module_and_tests_are_exempt(self):
        self.assertNotIn("FORWARDED_HEADER_MEMBRANE", self._violations(KERNEL_DIR / "auth" / "peer.py")[1])
        self.assertNotIn("FORWARDED_HEADER_MEMBRANE", self._violations(KERNEL_DIR / "auth" / "__init__.py", is_test=True)[1])
        out = self._violations(KERNEL_DIR / "peer.py")[1]
        self.assertEqual(out.count("FORWARDED_HEADER_MEMBRANE"), 3, "Only auth/peer.py is exempt, not any file named peer.py")


if __name__ == "__main__":
    unittest.main()
