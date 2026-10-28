# 01: Akasa Engineering Standards & Audit Guardrails

**Identity**: The Kernel Architect.  
**Mission**: To define the strict engineering principles, thread-safety models, VFS commit mechanics, and architectural guardrails governing the Akasa micro-kernel substrate.

---

## 0. Tier 1 Blind Substrate Mandate

Akasa is a strictly domain-agnostic Tier 1 application kernel and volume router.

* **Zero Domain Knowledge:** Akasa is completely blind to application-level concepts (LLMs, RAG context gathering, UI components, Git trees, task trackers, or code parsing). It exists purely to manage volume routing, stateless multi-tenancy, thread-local connection pooling, event dispatching, and background task execution.
* **Inversion of Control (IoC):** All application-specific capabilities must be injected into Akasa by host applications or Tier 2/3 extensions via `@hooks.on()` event subscriptions, custom VFS `Driver` registrations, and volume mount configurations.
* **Kernel Independence:** Akasa must build, test, and run without dependencies on higher-tier domain modules.

### 0.1 The 5-Ring Strict Acyclic Dependency DAG (`KERNEL_RING_DAG_MANDATE`)

To guarantee zero circular imports, deterministic initialization, and headless execution without HTTP/WSGI frameworks, all internal `akasa` modules are partitioned into five concentric rings (`Ring 0` through `Ring 4`):

* **Ring 0 — Pure Stdlib Primitives & Execution Context (`types.py`, `uri.py`, `context.py`):** Zero internal `akasa.*` imports and zero third-party dependencies. Houses `AkasaURI`, `ExecutionFrame` (`contextvars`), `atomic_write_bytes` / `atomic_write_text`, `@thread_safe_cache`, `clone_json_value`, `typeddict_to_openapi`, and pure mathematical/graph utilities.
* **Ring 1 — Signaling & Priority Event Bus (`events.py`, `hooks.py`):** Depends only on Ring 0. Houses `EventBus` (`sse_bus`) and `HookRegistry` (`hooks`). Delegates workspace enablement checks via an injected Ring 2 authorizer (`set_hook_authorizer`) so Ring 1 never imports upward.
* **Ring 2 — Persistence, VFS & Causal State (`db.py`, `vfs.py`, `state.py`, `utils.py`):** Depends only on Rings 0–1. `db.py` manages thread-local SQLite WAL pooling; `vfs.py` manages the volume router, `_VFS_READ_CACHE`, and `_VFS_PENDING_WRITES` overlay; `state.py` manages workspace physics (`get_workspace_physics`), `load_config`, and the causal parsed-object JSON cache (`_JSON_PARSED_CACHE` keyed by VFS revision tokens); `utils.py` serves as the unified backward-compatible facade re-exporting Ring 0 and Ring 2 primitives.
* **Ring 3 — Process Sandboxing, Worker Metronome & Heartbeat (`process.py`, `workers/__init__.py`, `sync.py`):** Depends only on Rings 0–2. Houses `ProcessManager`, `execute_binary`, dual-pool background workers (`Akasa-Interactive`, `Akasa-Worker`, `Akasa-IO`), and `get_system_deltas`.
* **Ring 4 — Transport Adapters, Cryptography & Extension SDK (`auth/__init__.py`, `extension.py`, `__init__.py`):** Depends on Rings 0–3. Houses Fernet secret management, `AkasaExtension`, `ExtensionContext`, `SettingsManager`, `JobManager`, and the optional Flask WSGI transport adapter (`akasa.init_app`).

**Enforced AST Fitness Invariants (`akasa/tests/fitness/rules_python.py`):**
1. **`KERNEL_RING_DAG_MANDATE`:** A module in Ring $N$ may only import from Ring $M$ where $M \le N$. Upward imports across rings are strictly banned; cross-ring coordination must use dependency injection callbacks bound at initialization time.
2. **`KERNEL_TRANSPORT_ISOLATION`:** Importing `flask` or WSGI transport objects is strictly banned in Rings 0–3 (`types`, `uri`, `context`, `events`, `hooks`, `db`, `vfs`, `state`, `utils`, `process`, `workers`, `sync`).
3. **`STRICT_TOP_LEVEL_IMPORTS_MANDATE`:** Inline `import` statements inside functions or methods are strictly prohibited across all kernel modules; all imports must reside at the top of the module.
4. **Subsystem `reset()` Contract:** Every stateful Ring module (`context`, `db`, `vfs`, `state`, `workers`) exposes a deterministic `reset()` function orchestrated by `akasa.reset_state()` for isolated unit test harnesses.

---

## 1. Single Source of Truth (SSOT) & Spatial Physics

Logic, pathing, and settings management must operate deterministically from a single source of truth across all tenant contexts.

* **Host-Agnostic Control Directory Resolution:** Hidden control directories must never be hardcoded as literal string paths (e.g. `".insetu"`). Control directory names must always resolve dynamically via `os.environ.get("AKASA_CONTROL_DIR", ".akasa")`.
* **Workspace Physics SSOT:** Physical workspace locations are resolved via `get_workspace_physics(workspace_id)`, returning a canonical 2-tuple `(config_path, workspace_root)`. System routing indexes reside in `<control_dir>/system.json`.
* **Volume-Sandboxed Subprocess Execution (`ProcessManager`):** Shell and subprocess operations must execute through `ProcessManager` (exposed on SDK context via `ctx.exec`), enforcing sandbox boundaries anchored to mounted volume targets with enforced timeouts.
* **Multi-Scoped Settings Matrix (`SettingsManager`):** Settings are managed statelessly across four explicit scopes:
  * `daemon`: System-wide preferences stored in `<control_dir>/host.json` (leaving `<control_dir>/workspaces.json` as the dedicated system routing and workspace physics index, healed automatically via `heal_daemon_config_files()`).
  * `workspace`: Tenant-specific settings stored in `<tenant_control_dir>/workspace.settings.json`.
  * `repo`: Repository-level settings stored in `<tenant_control_dir>/repo.settings.json`.
  * `secure`: Encrypted credentials stored in `<tenant_control_dir>/secrets.json` using Fernet cryptography driven by `~/.akasa/master_key.txt` (or isolated test sandbox directories via `_resolve_global_key_path()`).
* **Secret Masking & Master Key Rotation:** Reading encrypted settings via `SettingsManager.get(..., mask_secrets=True)` returns the sentinel `••••••••` to prevent plaintext credential exposure to client tiers. Write operations (`set`, `update`) drop masked sentinels to avoid overwriting stored secrets. Key rotation via `rotate_master_key()` re-encrypts all workspace `secrets.json` files and persists the new Fernet key atomically to `master_key.txt` with `0o600` permissions using `atomic_write_bytes()`, driven by the `security_settings_updated` hook.
* **Subprocess Environment Isolation:** Child processes spawned via `ProcessManager` or `execute_binary()` strictly sanitize inherited environment variables, stripping `AKASA_BOOT_TOKEN` from child environments to prevent credential leakage into untrusted subprocesses. Token evaluations in HTTP gates and bootstrap endpoints enforce constant-time string comparisons via `hmac.compare_digest()`.
* **Atomic Filesystem Writes (`atomic_write_bytes` / `atomic_write_text`):** Critical state mutations on host files (configuration files, master keys, and physical VFS writes) write to a dot-prefixed sibling temporary file, execute `os.fsync()`, preserve original filesystem mode permissions, and replace the target atomically via `os.replace()`.
* **Thread-Safe Single-Flight Cache Invalidation:** Any mutation to configuration structures or settings files on disk must emit a `config_mutated` event via `hooks.emit('config_mutated', workspace_id=...)`. Configuration loaders rely on `@thread_safe_cache` with single-flight execution barriers, returning shallow copies (`dict(...)` / `list(...)`) to eliminate `copy.deepcopy` serialization overhead, and race-condition protected invalidation hooks.
* **Defensive Fallbacks:** When accessing nested dictionary or array properties in configurations, getters must enforce explicit non-`None` fallbacks (e.g., `(cfg.get('key') or [])`) to prevent `TypeError` during iteration when keys are set to `null` in JSON payloads.

---

## 2. Stateless Multi-Tenancy & Thread-Local Storage

Akasa serves multiple tenant workspaces concurrently within a single process daemon without cross-tenant memory leakage.
* **Transport-Agnostic ExecutionFrame & Tenant Identification (`ExecutionFrame`):** Active tenant scope, transport type (`HTTP`, `IPC`, `WORKER`, `CLI`), and request payloads are bound to a slotted `ExecutionFrame` on a `contextvars.ContextVar` via `with execution_frame(workspace_id=..., transport=...):` in Ring 0 (`akasa/context.py`). `resolve_active_tenant()` and `sniff_tenant_id()` inspect the active `ExecutionFrame` first (ensuring explicit `ctx.call()` or worker frames always take precedence), falling back to the injected transport `tenant_resolver` (e.g., Flask's `X-Workspace-ID` header) and finally `"default"`.
* **Thread-Local Connection Pooling (`_local`):** SQLite connection pools are stored in thread-local storage (`threading.local()`) and structured hierarchically as `_local.connections[workspace_id][db_name]`. Connection limits are capped at 3 active workspaces per thread and 4 databases per workspace using True LRU eviction to prevent WAL handle exhaustion.
* **Thread-Local Connection Lifecycle Purging (`close_thread_connections`):** Per-request WSGI threads and background worker threads must call `close_thread_connections()` upon completing execution to close all cached SQLite connections on the calling thread, preventing file descriptor exhaustion. Individual connections can be evicted using `evict_cached_connection(workspace_id, db_name)`.
* **Database Relocation & Storage Bounds:** Tenant databases are stored in `<tenant_control_dir>/cache/db/<db_name>.db`. All connections must enforce:
  * Write-Ahead Logging (`PRAGMA journal_mode=WAL;`)
  * Normal synchronization (`PRAGMA synchronous=NORMAL;`)
  * Busy timeouts (`PRAGMA busy_timeout=5000;`)
  * In-memory temporary tables (`PRAGMA temp_store=MEMORY;`)
  * Bounded cache sizes (`PRAGMA cache_size=-2000;`)
* **Thread-Local Re-Entrancy Guards:** Event bus emissions utilize thread-local tracking (`_event_thread_local.active_events`) to catch and suppress recursive event loops on the same thread.

---

## 3. Asynchronous Virtual File System (VFS) Pipeline

Physical file I/O must never block main thread execution or risk partial file writes during concurrent operations.
* **Logical URI Boundaries (`AkasaURI`):** File locations are addressed using standard RFC 3986 URIs (`scheme://volume/path`). `AkasaURI` utilizes `__slots__` without dynamic `__dict__` allocations, normalizing paths to POSIX standard via `posixpath.normpath()` without path-segment stripping heuristics or double-slash leaks. The `.parts` property exposes immutable path components.
* **Strict Canonical URI Enforcement:** Operations within `VFSTransaction` (`save`, `delete`, `read`) enforce explicit scheme qualification (`://`). Relative or unqualified paths without a scheme prefix are strictly rejected with `ValueError` unless explicitly flagged with `is_absolute_artifact=True`.
* **Non-Blocking Micro-Batched Write Queue (`_VFS_WRITE_QUEUE`):** File mutations (`save`, `delete`, `move`, `bulk`) are enqueued onto a background commit pipeline thread (`_vfs_commit_worker`). The worker consumes write payloads in micro-batches of up to 64 items, utilizing a 5 ms coalescing window for non-barrier tasks to accumulate rapid producer bursts. Multi-mutation batches execute atomically in the background worker, batching SQLite ledger records (`executemany` on `vfs_event_log`) and emitting consolidated `vfs_mutated` background events to prevent event bus flooding.
* **Transaction Batch Aggregation (`VFSTransaction`):** Context-managed mutations buffer locally. Single-file operations fall back to standard `execute_vfs_save`, while multi-item buffers automatically consolidate into a single `execute_vfs_bulk` payload upon context exit (`__exit__`).
* **Pre-Flight Volume Validation:** Write operations (`execute_vfs_save` and `VFSTransaction.save`) validate that target volume paths resolve to a mounted volume prior to queuing. Unmounted volume target writes are dropped gracefully with telemetry warnings (`vfs_mount_missing`) to prevent invalid worker commits.
* **Pending-Write Overlay & Read-Your-Writes Consistency (`_VFS_PENDING_WRITES`):** When a mutation is enqueued onto `_VFS_WRITE_QUEUE`, the VFS synchronously records the staged content (or deletion tombstone) in an in-memory pending-write overlay prior to returning. `VFSTransaction.read()` inspects `_VFS_PENDING_WRITES` before consulting `_VFS_READ_CACHE` or the physical filesystem, guaranteeing immediate Read-Your-Writes consistency while background disk flushes are in flight.
* **In-Memory Write-Through LRU Read Cache (`_VFS_READ_CACHE`):** The VFS maintains an 8 MB thread-safe read cache (`_VFS_READ_CACHE`) for files up to 1 MB. Cache hits verify physical file metadata `(st_mtime_ns, st_size)` via a single `os.stat` call. Physical writes, moves, and deletions update or invalidate affected file paths and clear corresponding `_VFS_PENDING_WRITES` entries atomically upon disk commit.
* **Causal Mutation Hash Receipts (`base_hash`) & OCC Ring Buffer:** VFS save operations and `vfs_mutated` event emissions carry the SHA-256 `base_hash` of the written content so downstream observers, filesystem watchdogs, and client projections can deterministically identify self-echoes and ancestor states without time-window heuristics. Historical revisions are retained in an in-memory ancestor ring buffer (`_ANCESTOR_CACHE`, up to 12 revisions per file, capped at 1 MB per entry).
* **Deterministic 3-Way Text Merging & Zombie-ACK Detection:** Concurrent text edits are resolved through `three_way_merge_text(base_text, disk_text, incoming_text)`. Edit sequences are evaluated by `_is_subsequence_edit()` to identify Zombie-ACK patterns (where disk content represents an earlier snapshot of the identical incoming edit stream), fast-pathing the incoming change before falling back to pure-Python non-overlapping hunk merging.
* **Fast-Path Idempotency Gatekeeper:** Prior to disk I/O, `_is_file_identical()` evaluates physical `st_size` and memory cache text against proposed content. Identical payloads are dropped silently before disk writes or event ledger records are dispatched.
* **Inter-Workspace Virtual Mounts (`ws://`):** Peer tenant data is addressable using the `ws://<source_workspace_id>/<domain>/<relative_path>` URI scheme via `InterWorkspaceVFSDriver`. Target paths resolve strictly within `<source_workspace_dir>/data/<relative_path>`. Access is validated against the host-injected `_MOUNT_AUTHORIZER` callback (`set_mount_authorizer`). Cross-workspace mutations are strictly read-only and rejected with HTTP 403.
* **Queued Event-Driven Barrier Sentinel (`sync_vfs_barrier`):** Read-after-write synchronization is achieved by enqueuing an explicit `{"action": "barrier", "barrier_event": event}` sentinel onto `_VFS_WRITE_QUEUE`. Threads wait specifically on their barrier event, avoiding polling loops or CPU starvation. Upon releasing the VFS disk watermark, `sync_vfs_barrier` synchronizes with `hooks.sync_event_bus_barrier()` to guarantee that all spawned background event handlers have completed.
* **Auto-Healing Database Connections & Row Salvaging:** Connection pooling in `get_connection()` catches SQLite corruption errors (`DatabaseError` containing `malformed` or `corrupt`). Corrupt disk images are automatically quarantined to `.corrupt_<timestamp>`, re-initialized, and re-migrated cleanly. Prior to disposal, `recover_corrupt_databases()` attaches to quarantined dumps via SQLite `ATTACH DATABASE` to salvage undamaged rows using `INSERT OR IGNORE`. Quarantined database dumps (`*.db.corrupt_*`) older than 7 days are automatically garbage-collected by the Metronome worker engine (`sweep_ephemeral_artifacts`).
* **Declarative Schema Auto-Migration:** SQLite database tables required by kernel extensions register declarative schemas (`register_schema`) which auto-migrate on tenant boot via column diffing.

---

## 4. Asynchronous Metronome Worker Engine

Background tasks, sweeping routines, and scheduled execution must operate through the centralized Metronome ledger.

* **Database-Backed Job Ledgers:** Recurring metronome tasks are tracked in the `jobs` table; immediate off-thread tasks are tracked in `immediate_jobs`.
* **Explicit Lifecycle Scheduling:**
  * **Recurring Tasks:** Must be scheduled via `submit_job()` with an explicit `interval_ms > 0`.
  * **One-Shot Delayed Tasks:** Must be scheduled via `submit_one_shot_job()` with `delay_ms`. Upon execution, one-shot tasks automatically delete themselves from the ledger.
  * **Job State & Lifecycle Controls:** Scheduled tasks support deterministic state controls:
    * **Cancellation (`cancel_job`):** Purges the scheduled task record from the `jobs` ledger.
    * **Pause / Freeze (`pause_job`):** Transitions the job state to `paused` without dropping configured intervals or argument metadata.
    * **Resume (`resume_job`):** Restores `paused` jobs to `pending`, recalculating `next_run_at` based on `interval_ms` when `reset_timer=True`.
    * **SDK Exposure (`JobManager`):** Extensions manage tenant-scoped job lifecycles via `ctx.jobs.cancel()`, `ctx.jobs.pause()`, and `ctx.jobs.resume()`.
* **Unregistered Callback Auto-Suspension:** Scheduled jobs referencing unregistered Python callbacks automatically log a warning and suspend (`interval_ms = 0`), purging themselves from the ledger to prevent orphaned execution loops.
* **Job Step Chaining (ADR 0037 Contract):** Complex multi-stage background pipelines are declared using the `_chain` payload format:
  ```python
  {
      "steps": [
          {"ext_name": "module_a", "worker_name": "task_two"},
          {"ext_name": "module_b", "worker_name": "task_three"}
      ],
      "on_complete_hook": "pipeline_finished_event"
  }

```
* **Dual-Pool Concurrency & Worker Segregation:** The Metronome maintains two separate bounded thread pools:
  * `_executor` (8 threads, `Akasa-Worker`): Dedicated to recurring scheduled jobs, background workflows, and DAG pipeline steps.
  * `_interactive_executor` (4 threads, `Akasa-Interactive`): Dedicated strictly to non-DAG `ui_blocking` tasks, ensuring responsive user interactions are never queued behind heavy pipeline steps.
  * `_io_executor` (4 threads, `Akasa-IO`): Powers `run_parallel()` and SDK helper `ctx.jobs.map_parallel()`, guaranteeing deterministic thread-local connection cleanup (`close_thread_connections()`) upon worker completion. Nested invocations called from an executing `Akasa-IO` worker automatically degrade to sequential execution to prevent thread pool exhaustion and deadlock.
* **Signature-Driven Validation & Envelope Isolation:**
  * Worker submissions (`submit`, `submit_one_shot`, `schedule`, and `start_dag_chain`) validate keyword arguments against registered worker signatures and type annotations via `validate_worker_kwargs` and `validate_dag_chain_kwargs`. Arguments are checked recursively with `_check_type_match` (supporting `Literal`, `Union`, recursive sequences, mappings, strict differentiation of `bool` from `int`, and strict JSON serializability).
  * Pipeline envelope parameters (`root_job_id`, `chain_state`, `chain_history`, `_workspace_id`, `_chain`, `_dag_root_id`, `_dag_step_id`) are stripped from worker kwargs and isolated onto `ctx.jobs`, forwarding only accepted parameters to worker callbacks.
  * Disabled extension guards: recurring jobs belonging to disabled extensions are purged from the SQLite ledger upon dispatch, and immediate jobs fail gracefully with diagnostic error messages without executing callbacks.
* **Dynamic Condition-Variable Metronome Loop:** The Metronome dispatcher loop (`_metronome_loop`) synchronizes via `threading.Condition` (`_metronome_wakeup`). The loop computes the minimum `next_run_at` timestamp across all pending jobs, waiting on the condition variable until the nearest deadline or waking immediately when new tasks are scheduled via `submit_job()`, `submit_one_shot_job()`, or system shutdown. The loop handles shutdown gracefully, resetting in-flight task statuses to `pending` upon executor termination.
* **Kernel Memory Governance (`reclaim_process_memory`):** Heavy lifecycle events (DAG pipeline resolution, chained job completion, and ephemeral artifact sweeps) invoke `reclaim_process_memory()`, forcing Python garbage collection (`gc.collect()`) and releasing unmapped glibc arenas back to the host operating system via `malloc_trim(0)`.
* **Parallel DAG Chain Coordination (`start_dag_chain`):** Multi-stage background pipelines support parallel execution graphs via `start_dag_chain()` and `resolve_dag_chain()`. Root steps execute concurrently in the `ThreadPoolExecutor`. As steps complete, the coordinator evaluates downstream dependencies (`depends_on`), merges step outputs (`state_deltas`) into `chain_state`, emits `job_baton_passed` telemetry, and triggers `on_complete_hook` when all branches resolve.
* **In-Memory Job Barriers (`wait_for_jobs`):** Callers synchronize with in-flight immediate tasks via `wait_for_jobs(workspace_id, prefix=..., job_id=...)` using in-memory `threading.Event` sentinels (`_active_job_events`), avoiding polling overhead on the database ledger.
* **Argument-Based Job Coalescing:** `submit_immediate_job` supports `coalesce="args"`, matching active jobs with identical `(ext_name, callback_name, args_json)` to deduplicate identical compute workloads.

---

## 5. Event Bus Governance (`HookRegistry`)

Inter-module signaling and lifecycle notifications route through the synchronous/asynchronous `HookRegistry`.
* **Tenant Authorization & Memoization (`_is_authorized_cached`):** Before invoking a hook callback, the event bus verifies that the subscriber's module is enabled in the active workspace context (`is_extension_enabled`). Core system modules (`akasa.*`) and registered trusted host namespaces (`register_trusted_namespace`) bypass authorization checks. Unwrapped listener functions stamped with extension identifiers (`_akasa_ext_name`) resolve authorization in O(1) time. Authorization decisions are memoized in `_auth_cache` across `(callback, event_name, workspace_id)` and purged automatically when `config_mutated` is emitted.
* **Bounded Background Hook Execution & Connection Eviction:** Asynchronous event broadcasts via `hooks.emit_background()` route through a bounded `ThreadPoolExecutor` (`_bg_executor`, 4 workers, `Akasa-EventBus`), calling `close_thread_connections()` in a `finally` block on every worker thread to prevent handle leaks. Callers can synchronize with queued background events using `hooks.sync_event_bus_barrier(timeout=5.0)`.
* **Query-Response Hook Dispatchers:** Inter-module query workflows utilize explicit evaluators rather than manual callback iteration:
  * `emit_first()`: Evaluates authorized listeners in priority order, short-circuiting on the first truthy or matching response.
  * `emit_merge_dict()`: Aggregates dictionary payloads returned across authorized listeners into a consolidated dictionary.
  * `emit_merge_list()`: Flattens lists returned across authorized listeners into a unified list.
* **Bounded Background Hook Execution:** Asynchronous event broadcasts via `hooks.emit_background()` route through a bounded `ThreadPoolExecutor` (`_bg_executor`, 4 workers, `Akasa-EventBus`), preventing unbounded thread creation while falling back to daemon threads only during system shutdown.
* **Dependency-Injected Routing & Real-Time Broadcast:** Spatial volume routing functions can be injected via `set_workspace_router()`. UI signals and job progress updates are automatically dispatched to client queues via the global SSE bus (`sse_bus.emit_sse`). High-priority event passthroughs are registered dynamically via `register_sse_passthrough(*event_names)` rather than hardcoded in the kernel.
* **`system_boot` Boundary Protection:** Non-core domain extensions are explicitly banned from subscribing to `@hooks.on('system_boot')`. Domain extensions must subscribe to `@hooks.on('workspace_boot')` or `@hooks.on('topology_boot_complete')`.
* **Execution Telemetry:** Synchronous hook executions exceeding 5.0 seconds trigger telemetry warnings to identify slow handlers blocking event propagation.
* **Centralized Exception Dispatch:** Unhandled exceptions during event hook callbacks, background worker execution, or VFS write queue operations must emit background `system_error` events via `hooks.emit_background('system_error', ...)`.

---

## 6. Type Parity & Contract Specifications

All public SDK methods, event bus payloads, and worker interfaces must conform strictly to the canonical type contracts defined in `akasa/types.py`.
* **Canonical Types:** Use `WorkspaceID`, `FilePath`, `TenantPath`, `FileMutationPayload`, `JobProgressPayload`, `JobSchedulePayload`, `JobChainPayload`, `ChainStepSchema`, and `ChainStepArtifact`.
* **No Ambiguous Property Aliases:** Event payloads and API payloads must evaluate SSOT property names (`filepath` rather than ambiguous aliases like `path` or `filename`).
* **In-Process Zero-Copy IPC Gateway (`ctx.call`):** Extensions expose transport-agnostic capabilities registered automatically in `_CAPABILITY_REGISTRY`. Extensions invoke peer capabilities using `ctx.call(capability, wait=True, timeout=60.0, **kwargs)`, executing directly in-process within the active workspace context. Invocations returning HTTP 202 automatically await background worker settlement when `wait=True`. Input arguments and HTTP requests are unified via `ctx.payload` and `ctx.method`.
* **OpenAPI Schema Reflection:** API routes registered via `AkasaExtension.route()` must accept typed request/response schemas. `TypedDict` annotations are converted to OpenAPI 3.0 JSON Schema objects using `typeddict_to_openapi()`.

---

## 7. The Three-Tier Bug Diagnostic & Verification Governance

To prevent test suite bloat while maximizing defect prevention across the kernel, every bug or architectural regression discovered during development must pass through a strict triage decision tree before a fix is finalized:

```text
                                BUG DIAGNOSED
                                      │
        ┌──────────────────────────────┼──────────────────────────────┐
        ▼                              ▼                              ▼
1. Structural / Form           2. Behavioral Invariant        3. Isolated Logic
    (AST syntax, Ring DAG,         (Cross-system contracts,       (Math edge cases,
    inline imports, leaks)         idempotency, barriers)         URI/merge corner cases)
        │                              │                              │
        ▼                              ▼                              │
  Fitness Function               Contract / Matrix Unit Test    Local Regression Test
  (`tests/fitness/`)             (`tests/unit/test_*.py`)       (`tests/unit/test_*.py`)

```

1. **Structural Anti-Pattern $\rightarrow$ AST Fitness Function (`akasa/tests/fitness/`):**
* *Criterion:* If the defect is caused by a structural violation—such as an upward Ring import (`KERNEL_RING_DAG_MANDATE`), a transport leak in Rings 0–3 (`KERNEL_TRANSPORT_ISOLATION`), an inline function import (`STRICT_TOP_LEVEL_IMPORTS_MANDATE`), a synchronous file write outside `vfs.py` (`ASYNC_VFS_PIPELINE_COMPLIANCE`), or a domain token leak (`SUBSTRATE_PURITY_VIOLATION`)— that an LLM or developer could repeat in another file tomorrow, it belongs in `KernelFitnessVisitor`.
* *Verification:* Every new fitness rule must be paired with a negative synthetic AST test in `test_kernel_hardening.py` proving `KernelFitnessVisitor` catches the violation.


2. **Behavioral Invariant $\rightarrow$ Contract / Matrix Unit Test (`akasa/tests/unit/`):**
* *Criterion:* If the defect represents a breach of a cross-subsystem guarantee (e.g., Read-Your-Writes consistency between `_VFS_PENDING_WRITES` and `_JSON_PARSED_CACHE`, `ExecutionFrame` nesting precedence, worker signature reflection across complex `Union`/`Literal`/`Tuple` types, or subprocess `AKASA_BOOT_TOKEN` scrubbing across `run` and `popen` modes), codify it as a behavioral contract test in `akasa/tests/unit/`.


3. **Isolated Logic Flaw $\rightarrow$ Local Regression Test (`akasa/tests/unit/`):**
* *Criterion:* If the defect is an algorithmic edge case inside a single function (e.g., double-slash normalization in `AkasaURI` or subsequence detection in `three_way_merge_text`), add a targeted assertion to that subsystem's unit test method.



