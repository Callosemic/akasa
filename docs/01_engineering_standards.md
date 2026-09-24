# 01: Akasa Engineering Standards & Audit Guardrails

**Identity**: The Kernel Architect.  
**Mission**: To define the strict engineering principles, thread-safety models, VFS commit mechanics, and architectural guardrails governing the Akasa micro-kernel substrate.

---

## 0. Tier 1 Blind Substrate Mandate

Akasa is a strictly domain-agnostic Tier 1 application kernel and volume router.

* **Zero Domain Knowledge:** Akasa is completely blind to application-level concepts (LLMs, RAG context gathering, UI components, Git trees, task trackers, or code parsing). It exists purely to manage volume routing, stateless multi-tenancy, thread-local connection pooling, event dispatching, and background task execution.
* **Inversion of Control (IoC):** All application-specific capabilities must be injected into Akasa by host applications or Tier 2/3 extensions via `@hooks.on()` event subscriptions, custom VFS `Driver` registrations, and volume mount configurations.
* **Kernel Independence:** Akasa must build, test, and run without dependencies on higher-tier domain modules.

---

## 1. Single Source of Truth (SSOT) & Spatial Physics

Logic, pathing, and settings management must operate deterministically from a single source of truth across all tenant contexts.

* **Host-Agnostic Control Directory Resolution:** Hidden control directories must never be hardcoded as literal string paths (e.g. `".insetu"`). Control directory names must always resolve dynamically via `os.environ.get("AKASA_CONTROL_DIR", ".akasa")`.
* **Workspace Physics SSOT:** Physical workspace locations are resolved via `get_workspace_physics(workspace_id)`, returning a canonical 2-tuple `(config_path, workspace_root)`. System routing indexes reside in `<control_dir>/system.json`.
* **Volume-Sandboxed Subprocess Execution (`ProcessManager`):** Shell and subprocess operations must execute through `ProcessManager` (exposed on SDK context via `ctx.exec`), enforcing sandbox boundaries anchored to mounted volume targets with enforced timeouts.
* **Multi-Scoped Settings Matrix (`SettingsManager`):** Settings are managed statelessly across four explicit scopes:
  * `daemon`: System-wide preferences stored in `<control_dir>/system.json`.
  * `workspace`: Tenant-specific settings stored in `<tenant_control_dir>/workspace.settings.json`.
  * `repo`: Repository-level settings stored in `<tenant_control_dir>/repo.settings.json`.
  * `secure`: Encrypted credentials stored in `<tenant_control_dir>/secrets.json` using Fernet cryptography driven by `~/.akasa/master_key.txt`.
* **Thread-Safe Single-Flight Cache Invalidation:** Any mutation to configuration structures or settings files on disk must emit a `config_mutated` event via `hooks.emit('config_mutated', workspace_id=...)`. Configuration loaders rely on `@thread_safe_cache` with single-flight execution barriers and race-condition protected invalidation hooks.
* **Defensive Fallbacks:** When accessing nested dictionary or array properties in configurations, getters must enforce explicit non-`None` fallbacks (e.g., `(cfg.get('key') or [])`) to prevent `TypeError` during iteration when keys are set to `null` in JSON payloads.

---

## 2. Stateless Multi-Tenancy & Thread-Local Storage

Akasa serves multiple tenant workspaces concurrently within a single process daemon without cross-tenant memory leakage.

* **Request-Scoped Workspace Identification:** Tenant contexts are passed explicitly or extracted via the `X-Workspace-ID` HTTP header (`sniff_tenant_id()`). Default fallback is always `"default"`.
* **Thread-Local Connection Pooling (`_local`):** SQLite connection pools are stored in thread-local storage (`threading.local()`) and keyed by `(workspace_id, db_name)`. Connection limits are capped at 5 active workspaces per thread using True LRU eviction to prevent WAL handle exhaustion.
* **Database Safety Standards:** All SQLite handles must enforce:
  * Write-Ahead Logging (`PRAGMA journal_mode=WAL;`)
  * Normal synchronization (`PRAGMA synchronous=NORMAL;`)
  * Busy timeouts (`PRAGMA busy_timeout=5000;`)
* **Thread-Local Re-Entrancy Guards:** Event bus emissions utilize thread-local tracking (`_event_thread_local.active_events`) to catch and suppress recursive event loops on the same thread.

---

## 3. Asynchronous Virtual File System (VFS) Pipeline

Physical file I/O must never block main thread execution or risk partial file writes during concurrent operations.

* **Logical URI Boundaries (`AkasaURI`):** File locations are addressed using standard RFC 3986 URIs (`scheme://volume/path`). Paths are normalized to POSIX standard without path-segment stripping heuristics.
* **Non-Blocking Write Queue (`_VFS_WRITE_QUEUE`):** File mutations (`save`, `delete`, `move`) are enqueued onto a background commit pipeline thread (`_vfs_commit_worker`). REST and SDK endpoints accept writes asynchronously and return HTTP 202 status confirmations.
* **Idempotency Gatekeeper:** Prior to writing to disk, the VFS worker evaluates existing disk contents. If the payload is identical, the disk write and subsequent event ledger updates are silently dropped to eliminate phantom I/O events.
* **Queued Event-Driven Barrier Sentinel (`sync_vfs_barrier`):** Read-after-write synchronization is achieved by enqueuing an explicit `{"action": "barrier", "barrier_event": event}` sentinel onto `_VFS_WRITE_QUEUE`. Threads wait specifically on their barrier event, avoiding polling loops or CPU starvation.
* **Auto-Healing Database Connections:** Connection pooling in `get_connection()` runs `PRAGMA quick_check(1)` upon connection. Corrupt disk images are automatically quarantined to `.corrupt_<timestamp>`, re-initialized, and re-migrated cleanly.
* **Declarative Schema Auto-Migration:** SQLite database tables required by kernel extensions register declarative schemas (`register_schema`) which auto-migrate on tenant boot via column diffing.

---

## 4. Asynchronous Metronome Worker Engine

Background tasks, sweeping routines, and scheduled execution must operate through the centralized Metronome ledger.

* **Database-Backed Job Ledgers:** Recurring metronome tasks are tracked in the `jobs` table; immediate off-thread tasks are tracked in `immediate_jobs`.
* **Explicit Lifecycle Scheduling:**
  * **Recurring Tasks:** Must be scheduled via `submit_job()` with an explicit `interval_ms > 0`.
  * **One-Shot Delayed Tasks:** Must be scheduled via `submit_one_shot_job()` with `delay_ms`. Upon execution, one-shot tasks automatically delete themselves from the ledger.
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

* **Metronome Loop Resilience:** The Metronome thread queries pending jobs across all registered tenant workspaces in `system.json`, handing ready callbacks off to a constrained `ThreadPoolExecutor`.

---

## 5. Event Bus Governance (`HookRegistry`)

Inter-module signaling and lifecycle notifications route through the synchronous/asynchronous `HookRegistry`.
* **Tenant Authorization (`_is_authorized`):** Before invoking a hook callback, the event bus verifies that the subscriber's module is enabled in the active workspace context (`is_extension_enabled`). Core system modules (`akasa.*`) and registered trusted host namespaces (`register_trusted_namespace`) bypass authorization checks.
* **Dependency-Injected Routing & Real-Time Broadcast:** Spatial volume routing functions can be injected via `set_workspace_router()`. UI signals and job progress updates are automatically dispatched to client queues via the global SSE bus (`sse_bus.emit_sse`).
* **`system_boot` Boundary Protection:** Non-core domain extensions are explicitly banned from subscribing to `@hooks.on('system_boot')`. Domain extensions must subscribe to `@hooks.on('workspace_boot')` or `@hooks.on('topology_boot_complete')`.
* **Execution Telemetry:** Synchronous hook executions exceeding 5.0 seconds trigger telemetry warnings to identify slow handlers blocking event propagation.
* **Centralized Exception Dispatch:** Unhandled exceptions during event hook callbacks, background worker execution, or VFS write queue operations must emit background `system_error` events via `hooks.emit_background('system_error', ...)`.

---

## 6. Type Parity & Contract Specifications

All public SDK methods, event bus payloads, and worker interfaces must conform strictly to the canonical type contracts defined in `akasa/types.py`.
* **Canonical Types:** Use `WorkspaceID`, `FilePath`, `TenantPath`, `FileMutationPayload`, `JobProgressPayload`, `JobSchedulePayload`, `JobChainPayload`, `ChainStepSchema`, and `ChainStepArtifact`.
* **No Ambiguous Property Aliases:** Event payloads and API payloads must evaluate SSOT property names (`filepath` rather than ambiguous aliases like `path` or `filename`).
* **OpenAPI Schema Reflection:** API routes registered via `AkasaExtension.route()` must accept typed request/response schemas. `TypedDict` annotations are converted to OpenAPI 3.0 JSON Schema objects using `typeddict_to_openapi()`.
