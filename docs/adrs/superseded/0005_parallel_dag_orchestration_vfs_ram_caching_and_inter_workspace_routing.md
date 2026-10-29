# ADR 0005: Parallel DAG Orchestration, In-Memory VFS Read Caching, and Inter-Workspace Routing

* **Status:** Superseded by [ADR 0006](../0006_resource_governance_bounded_executors_and_secret_lifecycle.md), [ADR 0008](../0008_microbatched_vfs_dual_pool_workers_and_ipc_gateway.md), and [ADR 0010](../0010_fail_closed_peer_trust_dns_rebinding_gate_and_dag_wavefronts.md)
* **Date:** 2026-10-18
* **Superseded Date:** 2026-10-28
* **Scope:** Akasa Micro-Kernel Architecture, VFS Pipeline, Worker Metronome, & Connection Pool

## Context
As workspace interactions and compilation pipelines grew more complex, four structural limitations emerged:
1. **Linear Workflow Execution:** Background pipeline chaining was strictly sequential (`_chain`), unable to model tasks with independent prerequisite branches or parallel fan-out/fan-in steps.
2. **Disk I/O Bottlenecks on Repeated Reads:** High-frequency file reads during AST sweeps and settings lookups produced continuous disk I/O without in-memory buffering.
3. **Cross-Tenant Data Isolation Barriers:** Inter-workspace data sharing lacked a safe, virtualized abstraction, forcing higher-tier applications to use manual path resolution or host-level filesystem coupling.
4. **Connection Pool Scalability:** Flat `(workspace_id, db_name)` connection caching in `_local` evicted entire tenant connections prematurely when switching databases within the same tenant.

## Decision
1. **Parallel DAG Orchestrator:** Implemented `resolve_dag_chain()` for cycle-safe topological sorting (with anchor ordering: `source`, `body`, `sink`) and `start_dag_chain()` with `_dag_coordinators` state tracking. Dispatches root steps concurrently, evaluates prerequisites dynamically on step completion, and merges step `state_deltas` into `chain_state`.
2. **In-Memory VFS LRU Read Cache:** Added a 32 MB write-through RAM cache (`_VFS_READ_CACHE`) for files up to 2 MB. Cache hits evaluate physical `(st_mtime_ns, st_size)` via a single stat syscall. Integrated `_is_file_identical()` to short-circuit idempotent writes before touching disk or the event bus.
3. **Inter-Workspace Virtual Mounts (`ws://`):** Introduced `InterWorkspaceVFSDriver` mapping `ws://<workspace_id>/<domain>/<rel_path>` to `<source_tenant_control_dir>/data/<rel_path>`. Access is validated against the host-injected `_MOUNT_AUTHORIZER` callback. All mutations to `ws://` URIs are rejected as read-only.
4. **Two-Tier Thread-Local Connection Pooling:** Grouped connections hierarchically by `workspace_id -> db_name` with LRU limits (3 workspaces, 4 DBs per workspace) and `PRAGMA cache_size=-8000;`. Provided `close_thread_connections()` for WSGI and worker thread cleanup.
5. **Condition-Variable Metronome Loop:** Converted `_metronome_loop` to use `threading.Condition` (`_metronome_wakeup`), replacing 1-second busy polling with dynamic sleep intervals calculated from active task schedules.

## Consequences
* Workflows can execute independent processing branches simultaneously and synchronize outputs cleanly.
* Hot file read latency is reduced significantly through memory caching.
* Peer tenants can exchange data safely across an authorized virtual URI layer without filesystem path leaking.
* Thread-local SQLite handle management eliminates connection churn and file descriptor leaks.