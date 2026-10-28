# ADR 0009: 5-Ring Strict Acyclic Kernel DAG, ExecutionFrame ContextVars, Inter-Process State Locking, and Zero-Copy Causal Config Caching

* **Status:** Accepted
* **Date:** 2026-10-27
* **Scope:** Akasa Micro-Kernel Architecture, Dependency Topology, Execution Context, State Caching, & Fitness Governance

## Context
As the Akasa micro-kernel evolved into a multi-tenant runtime supporting both headless daemon execution and embedded host integrations, five architectural friction points emerged:
1. **Module Coupling & Circular Dependencies**: Stateful logic was concentrated in `akasa.utils`, leading to cyclical import hazards between `db`, `vfs`, `hooks`, and `extension`. Modules frequently relied on inline imports inside functions to defer import cycles.
2. **Ambient Tenant Coupling to HTTP Requests**: Tenant identification (`sniff_tenant_id`) relied directly on Flask's `request` context. Headless background workers, CLI commands, and in-process IPC calls lacked a native, transport-agnostic execution context.
3. **Control-Plane Inter-Process Race Conditions**: While VFS file writes were non-blocking and atomic, concurrent processes modifying control-plane configurations (`workspaces.json`, `host.json`) risked clobbering file content during simultaneous mutations.
4. **JSON Parsing & Deepcopy Overhead on Configuration Reads**: `load_json_config` re-read or re-parsed JSON configurations from disk or raw text strings, incurring significant deserialization overhead during high-frequency settings reads.
5. **Read-Your-Writes Fragmentation**: Multiple subsystems implemented independent checks across disk stat signatures, LRU caches, and in-memory pending-write overlays, lacking a single canonical primitive for immediate read consistency.

## Decision
1. **5-Ring Strict Acyclic Dependency Architecture (`KERNEL_RING_DAG_MANDATE`)**:
   - Partitioned the kernel into five strictly bounded concentric rings:
     - **Ring 0 (Pure Stdlib Primitives & Execution Context)**: `types.py`, `uri.py`, `context.py`. Zero internal `akasa.*` imports and zero third-party dependencies.
     - **Ring 1 (Signaling & Priority Event Bus)**: `events.py`, `hooks.py`.
     - **Ring 2 (Persistence, VFS & Causal State)**: `db.py`, `vfs.py`, `state.py`, `utils.py` (facade).
     - **Ring 3 (Sandboxed Process Execution, Worker Metronome & Sync)**: `process.py`, `workers/__init__.py`, `sync.py`.
     - **Ring 4 (Transport Adapters, Security & Extension SDK)**: `auth/__init__.py`, `extension.py`, `__init__.py`.
   - Prohibited upward imports across rings ($M > N$) and banned all inline function imports in kernel modules.
2. **`ExecutionFrame` & `ContextVar`-Backed Tenant Resolution (`akasa.context`)**:
   - Implemented a slotted, immutable `ExecutionFrame` stored on a `contextvars.ContextVar`.
   - Provided `execution_frame(workspace_id, transport, method, payload, job_id)` context manager, guaranteeing that active tenant and payload contexts propagate cleanly across sync and async calls.
   - Introduced `set_tenant_resolver()` and `resolve_active_tenant()`, completely decoupling `sniff_tenant_id` from Flask.
3. **Inter-Process File Locking (`_file_write_lock`)**:
   - Added thread and inter-process locking on `<control_dir>/.state.lock` (using `fcntl.flock` on POSIX and `msvcrt.locking` on Windows) within `atomic_write_bytes` and `atomic_write_text` to serialize control-plane state mutations safely across separate OS processes.
4. **Causal Parsed-Object JSON Cache (`akasa.state`)**:
   - Implemented `_JSON_PARSED_CACHE` in `akasa.state`, caching parsed JSON data structures keyed by VFS causal revision tokens (`-pending_seq` or `st_mtime_ns`). Cache hits return fast container clones (`clone_json_value`) without executing `json.loads()` or deepcopy.
5. **Unified Read-Your-Writes Primitive (`read_cached_physical_text`)**:
   - Consolidated physical text reads into `read_cached_physical_text()` in `akasa.vfs`. The primitive inspects `_VFS_PENDING_WRITES` first, then `_VFS_READ_CACHE`, and finally disk with a single stat call.
6. **Acyclic Dependency Injection (DI) Wiring**:
   - Added `hooks.set_hook_authorizer()` and `hooks.set_thread_cleanup()` to eliminate Ring 1 imports of Ring 2.
   - Added `set_db_physics_resolvers()` in `akasa.db` and `set_vfs_artifact_path_resolver()` in `akasa.vfs` to eliminate upward dependencies on `akasa.state`.
   - Standardized `reset()` contracts across all stateful subsystems (`context`, `db`, `vfs`, `state`, `workers`), orchestrated by `akasa.reset_state()`.
7. **Ephemeral Scratch Buffer SDK (`ctx.jobs.write_scratch`)**:
   - Added `write_scratch(filename, content, ttl_seconds)` to `JobManager`, allowing background workers to atomically write temporary output buffers to `<control_dir>/cache/<ext>/scratch/` with automatic TTL cleanup registration.

## Consequences
* The kernel dependency graph is strictly acyclic, eliminating circular import hazards and making startup deterministic.
* Tenant scope and request execution context are completely independent of HTTP transport frameworks.
* Multi-process daemon access to configuration files is safe from race conditions.
* High-frequency configuration lookups execute at memory speed without JSON parsing bottlenecks.
* Unit test suites run with total isolation via unified subsystem reset contracts.