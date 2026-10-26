# ADR 0008: Micro-Batched VFS Commits, Dual-Pool Worker Scheduling, Signature-Driven Type Validation, and Zero-Copy IPC Gateway

* **Status:** Accepted
* **Date:** 2026-10-26
* **Scope:** Akasa Micro-Kernel Architecture, VFS Pipeline, Worker Engine, Type Safety, and Extension SDK

## Context
As the Akasa micro-kernel scaled to orchestrate high-velocity file operations and multi-extension workflows, four structural friction points surfaced:
1. **SQLite Ledger Thrashing During Concurrent File Bursts**: While individual transactions were non-blocking, rapid sequential file saves generated separate queue items on `_VFS_WRITE_QUEUE`, causing thread contention and redundant individual commits to `vfs_event_log`.
2. **Interactive UI Latency Contention**: Immediate background tasks shared a single 8-worker thread pool (`Akasa-Worker`). When heavy compilation DAG chains saturated the pool, user-initiated interactive actions (`ui_blocking`) were delayed behind long-running background tasks.
3. **Runtime Type Drift in Background Workers**: Metronome worker arguments were passed as unvalidated JSON dictionaries. Subtle type mismatches (such as passing lists to boolean flags, or invalid enum literals) propagated deep into extension pipelines before failing, lacking fail-fast synchronous validation. Furthermore, pipeline envelope state (`root_job_id`, `chain_state`, `chain_history`) polluted worker argument signatures.
4. **Inter-Extension IPC Overhead**: Extensions needing to invoke capabilities on peer extensions were forced to route through HTTP localhost networking, incurring network serialization and authentication overhead for purely in-process operations.
5. **Memory Overhead in Path Parsing**: `AkasaURI` instances allocated standard Python instance dictionaries (`__dict__`) and relied on URL parsing libraries, introducing allocation overhead during high-frequency directory walking and path matching.

## Decision
1. **Asynchronous VFS Pipeline Micro-Batching (`_vfs_commit_worker`)**:
   - Refactored `_vfs_commit_worker` to drain `_VFS_WRITE_QUEUE` in micro-batches of up to 64 operations with a 5 ms coalescing window for non-barrier tasks.
   - Grouped ledger writes per workspace, executing batched `executemany` inserts into `vfs_event_log`.
   - Consolidated `vfs_mutated` notifications into a single background emission per workspace.
   - Integrated `atomic_write_text` (dot-prefixed temporary file, `os.fsync`, mode preservation, and `os.replace`) into physical disk saves.
   - Short-circuited idempotent file writes pre-flight using `_is_file_identical` before touching disk or queuing events.
2. **Dual-Pool Worker Metronome**:
   - Introduced `_interactive_executor` (4 workers, `Akasa-Interactive`) strictly dedicated to non-DAG `ui_blocking` tasks, reserving `_executor` (8 workers, `Akasa-Worker`) for recurring tasks, DAG steps, and system background processing.
3. **Signature-Driven Parameter Validation & Envelope Isolation**:
   - Implemented `validate_worker_kwargs` and `validate_dag_chain_kwargs` driven by signature reflection and `_check_type_match`. Submissions fail fast with descriptive `TypeError` exceptions for invalid types, unknown keyword arguments, or non-JSON-serializable objects.
   - Enforced strict differentiation between `bool` and `int` (`isinstance(True, int)` protection).
   - In `AkasaExtension.worker`, stripped pipeline envelope keys (`root_job_id`, `chain_state`, `chain_history`, `_workspace_id`, `_chain`, `_dag_root_id`, `_dag_step_id`) and isolated them onto `ctx.jobs`, passing only explicitly declared parameters to callback functions.
   - Enforced disabled-extension rules: recurring jobs belonging to disabled extensions are deleted from the ledger upon dispatch, and immediate jobs fail safely without executing callbacks.
4. **In-Process Zero-Copy IPC Capability Gateway (`ctx.call`)**:
   - Established `_CAPABILITY_REGISTRY` registering transport-agnostic capabilities (`<ext>.<action>`).
   - Implemented `ExtensionContext.call(capability, wait=True, timeout=60.0, **kwargs)`, enabling direct in-process capability execution between extensions across workspace tenant boundaries. Automatically awaits background 202 jobs when `wait=True`.
   - Unified input handling across HTTP requests and in-process calls via `ctx.payload` and `ctx.method`.
5. **Slotted Zero-Allocation `AkasaURI` Parser**:
   - Converted `AkasaURI` to `__slots__ = ('scheme', 'volume', 'path', 'is_dir', '_cached_str')`, eliminating per-instance `__dict__` overhead.
   - Replaced URL parsing with pure string splitting and `posixpath.normpath` normalization, preventing double slashes and POSIX root leaks. Added the `.parts` tuple property.
6. **Hook Authorization Memoization & Event Bus Barrier**:
   - Implemented `_auth_cache` in `HookRegistry` to memoize tenant authorization decisions in O(1) time, automatically cleared when `config_mutated` fires.
   - Added `hooks.sync_event_bus_barrier()` awaiting background event completion, directly integrated into `sync_vfs_barrier`.
   - Guaranteed `close_thread_connections()` execution in a `finally` block on all background hook threads.
7. **Security & State Hardening**:
   - Stripped `AKASA_BOOT_TOKEN` from child environments in `execute_binary`.
   - Isolated master key resolution via `_resolve_global_key_path` and `_in_test_sandbox` flag to protect home directories during unit tests.
   - Standardized token verification on constant-time `hmac.compare_digest`.
   - Added `PRAGMA temp_store=MEMORY;` across all tenant SQLite connections.

## Consequences
* High-frequency file write bursts commit with minimal SQLite lock contention and reduced event bus noise.
* Interactive UI requests execute without latency spikes caused by background compilation DAGs.
* Worker parameters are strictly validated at invocation time, eliminating silent type drift and pipeline envelope pollution.
* Extensions communicate cleanly in-process without network hops or HTTP serialization overhead.
* Path operations allocate minimal heap memory, and master keys are protected from unit test pollution.