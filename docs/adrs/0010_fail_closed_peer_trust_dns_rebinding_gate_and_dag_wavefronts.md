# ADR 0010: Fail-Closed Peer Classification, DNS-Rebinding Host Gate, Topological DAG Wavefronts, and Test Sandbox Quiescing

* **Status:** Accepted
* **Date:** 2026-10-28
* **Scope:** Akasa Micro-Kernel Architecture, Security Membrane, Worker Engine, & Test Lifecycle

## Context
As the Akasa micro-kernel scaled to serve web interfaces, CLI commands, background daemons, and multi-tenant test suites, four structural concerns emerged:
1. **Ambient Proxy Spoofing & Forwarded Header Drift**: Request origin detection relied on reading `X-Forwarded-For` or `request.remote_addr` directly within authentication gates. Spoofed leftmost entries in forwarding headers risked granting localhost trust to remote clients behind reverse proxies. Host-level code also read forwarding headers ad-hoc without uniform fail-closed guarantees.
2. **DNS-Rebinding Vulnerability**: While token authentication guarded private routes, endpoints answering requests before token verification (such as public routes or `/auth/bootstrap`) did not validate the HTTP `Host` header against loopback addresses or trusted hostnames, exposing local services to DNS-rebinding attacks from malicious browsers.
3. **DAG Chaining Complexity & Ad-Hoc Topological Sorting**: Multi-stage background pipelines in `akasa.workers` relied on recursive DFS sorting with manual cycle tracking, lacking deterministic level-order generation wavefronts and stdlib topological sorting.
4. **Test Harness Concurrency Leaks & CWD Relocation**: During unit testing, `akasa.reset_state()` switched daemon working directories (`_cwd`) while background Metronome threads and async VFS write workers were still in flight. Un-drained VFS writes queued in one sandbox occasionally flushed to disk after `cwd` had switched, polluting adjacent test directories. Furthermore, `run_parallel()` nested invocations risked deadlocking the bounded `Akasa-IO` thread pool.

## Decision
1. **Ring 4 Peer Classification Membrane (`akasa.auth.peer`)**:
   - Introduced `classify_peer(remote_addr, headers)` and the immutable `PeerInfo` dataclass as the single source of truth for request origin evaluation.
   - Enforced fail-closed proxy trust: the presence of any forwarding header (`Forwarded`, `X-Forwarded-*`, `X-Real-IP`, or registered custom proxy headers) marks the request as proxied and unconditionally revokes `is_direct_loopback` trust.
   - Enforced rightmost-hop IP resolution: only the rightmost IP in `X-Forwarded-For` (appended by the local machine's reverse proxy) is trusted as the client address. Leftmost client-supplied entries are ignored.
   - Codified `FORWARDED_HEADER_MEMBRANE` in `KernelFitnessVisitor`: direct inspection of client-identity forwarding headers is strictly prohibited outside `akasa/auth/peer.py`.
2. **DNS-Rebinding Host Gatekeeper (`akasa.__init__`, `akasa.auth.peer`)**:
   - Implemented `is_host_allowed(host, trusted_suffixes, allowed_hosts)` and `is_loopback_origin(url_str)`.
   - Wired `enforce_host_validation` (default `True`) into `@app.before_request` in `init_app()`, rejecting any foreign Host header with HTTP `421 Misdirected Request` prior to route matching or token gate evaluation.
   - Restricted localhost bypass in `/auth/bootstrap` to unproxied requests with loopback Host and loopback Origin/Referer.
3. **Ring 0 Level-Order DAG Wavefronts & Template Engines (`akasa.context`)**:
   - Implemented `resolve_dag_wavefronts()` in Ring 0 using standard library `graphlib.TopologicalSorter`, producing deterministic topological generations with secondary order tie-breaking. Refactored `resolve_dag_chain()` in `akasa.workers` to delegate directly to this primitive.
   - Implemented stdlib template parsing and substitution primitives (`extract_template_variables`, `interpolate_template_variables`) in Ring 0 without third-party template engine dependencies.
4. **Test Sandbox Quiescing & Re-Entrant Worker Safeguards (`akasa.workers`, `akasa.vfs`)**:
   - Implemented `_quiesce_background_workers()`, joining the Metronome thread and shutting down both worker thread pools (`cancel_futures=True`) during `workers.reset()`, while re-arming lazy restart for subsequent tasks.
   - Hardened `vfs.reset()` to execute `sync_vfs_barrier("default", timeout=5.0)` before clearing in-memory caches, guaranteeing all queued writes commit under the sandbox working directory where they were scheduled.
   - Added thread-local re-entrancy tracking (`_io_pool_local.in_pool`) in `run_parallel()`, falling back to synchronous inline execution when called from within an active `Akasa-IO` thread to prevent worker starvation and deadlocks.
5. **Explicit Settings State Introspection (`SettingsManager.is_set`)**:
   - Added `is_set(key, repo=None)` to `SettingsManager` to allow callers to unambiguously distinguish explicitly persisted values from schema default fallbacks across daemon, workspace, repo, and secure scopes.

## Consequences
* Origin classification and proxy trust fail closed across all transports, eliminating IP-spoofing vectors.
* The daemon is protected against DNS-rebinding attacks on all HTTP endpoints.
* Forwarding headers cannot be read outside `akasa/auth/peer.py`, preventing membrane drift.
* Topological sorting relies on standard library primitives with deterministic wavefront evaluation.
* Test suites execute in total isolation without thread leaks, database collisions, or stray VFS writes across directory swaps.
