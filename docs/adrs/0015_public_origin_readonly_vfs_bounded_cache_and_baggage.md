# ADR 0015: Public-Origin Read-Only VFS Boundary, Ring 0 BoundedCache, ExecutionFrame Baggage Propagation, and MemoryVFSTransaction

* **Status:** Accepted
* **Date:** 2026-11-18
* **Scope:** Akasa Security Gate, Ring 0 Caching & ExecutionFrame, VFS Contract, & Worker Concurrency

## Context
As Akasa evolved to support unauthenticated public endpoints alongside authenticated sessions, bounded in-memory caching, and multi-hop background execution pipelines, four structural needs emerged:
1. **Read-Only Enforcement on Public Endpoints:** Host applications expose read-only public routes (`public_routes`, `public_prefixes`, `public_suffixes`) without session credentials. Without an explicit origin marker on unauthenticated public requests, downstream helpers or shared routines lacked a kernel-enforced guarantee preventing accidental VFS writes.
2. **Ad-Hoc Unbounded Cache Dictionaries:** `akasa/state.py` maintained separate `_JSON_CACHE`, `_JSON_MTIME`, and `_JSON_PARSED_CACHE` dictionaries with manual locking and unbounded growth, while lacking a reusable Ring 0 LRU cache with causal revision validation and automatic `reset_state()` cleanup.
3. **Context Propagation Across `run_parallel()` and Worker Envelopes:** `ThreadPoolExecutor.submit()` in `run_parallel()` dropped caller `contextvars`, losing active tenant and origin context inside `Akasa-IO` threads. Additionally, callers needed domain-agnostic `baggage` propagation across nested frames and background jobs without polluting worker function signatures.
4. **Pure In-Memory VFS Testing & Causal Revision Reads:** Callers and test harnesses needed `VFSTransaction.read_with_revision()` to inspect causal revision tokens alongside file contents, as well as a canonical in-memory `MemoryVFSTransaction` implementing the full VFS contract without disk I/O.

## Decision
1. **Public-Origin Read-Only VFS Enforcement (`akasa/__init__.py`, `akasa/vfs.py`):**
   - In `init_app()`, requests matching `public_routes`, `public_prefixes`, or `public_suffixes` (excluding `/auth/bootstrap`) without a valid boot/session token enter an `ExecutionFrame(transport="HTTP", method=request.method, origin="public")` in `@app.before_request` and exit on `@app.teardown_request`. Authenticated requests to public routes retain their normal session context.
   - `execute_vfs_save()`, `execute_vfs_delete()`, `execute_vfs_move()`, and `execute_vfs_bulk()` reject all mutations with `"Public routes are strictly read-only."` (`403`) whenever `get_current_origin() == "public"`.
   - `public` is sticky: `execution_frame()` keeps `origin="public"` for any frame nested under a public frame, even when the nested frame names another origin, so a host dispatcher can't escalate an unauthenticated request.
2. **Ring 0 `BoundedCache` (`akasa/context.py`, `akasa/state.py`):**
   - Implemented `BoundedCache(name, max_entries=512, ttl=None, copy_mode="json")` in Ring 0 with `OrderedDict` LRU eviction, causal `revision` token matching, monotonic TTL expiration, defensive cloning (`"json"`, `"deepcopy"`, `"none"`), `approx_kb()` telemetry, `get_registered_cache_stats()`, and automatic clearing in `akasa.context.reset()`.
   - Replaced ad-hoc cache dicts in `akasa/state.py` with `_JSON_CONFIG_CACHE = BoundedCache("json_config", max_entries=256)`.
3. **`ExecutionFrame.baggage` & `contextvars` Propagation (`akasa/context.py`, `akasa/workers/__init__.py`):**
   - Added `baggage` to `ExecutionFrame` and `get_current_baggage()`, merging non-empty keys across nested `execution_frame()` blocks.
   - Updated `run_parallel()` to submit tasks via `contextvars.copy_context().run`, preserving `ExecutionFrame` (`workspace_id`, `origin`, `baggage`) across `Akasa-IO` threads.
   - Added `_origin` and `_baggage` to `_ENVELOPE_KEYS`, stamping the active submitter's origin and baggage in `_prepare_job_payload()` and restoring them in `_execute_immediate_job()` and `_execute_job()`. The submitting frame always wins: caller-supplied `_origin` / `_baggage` in job kwargs are overwritten, so a job can't claim an origin its submitter doesn't have.
4. **`read_with_revision()` & `MemoryVFSTransaction` (`akasa/vfs.py`):**
   - Added `VFSTransaction.read_with_revision()` returning `(revision_token, content)` with Read-Your-Writes consistency.
   - Added `MemoryVFSTransaction(VFSTransaction)` backed by an in-memory dictionary (`files`), implementing the full `VFSTransaction` interface without disk I/O.
5. **Unified Schema Migration on Connection Open (`akasa/db.py`):**
   - Consolidated `CREATE TABLE`, `ALTER TABLE ADD COLUMN` diffing, and `CREATE INDEX` execution into `_apply_schema_to_conn()`, invoked by both `apply_declarative_schema()` and `get_connection()`, tracked via thread-local `_local.applied_schemas` with deterministic cleanup on connection close/eviction.

## Consequences
* Unauthenticated public routes can't write through the VFS entry points from code that stays in the request's frame, including `run_parallel()` tasks and jobs the request submits. As with ADR 0014, an origin is a label, not an authentication: SQLite writes, raw threads started without the caller's context, and direct calls to `execute_vfs_save_physical()` are outside the guard. It keeps public routes honest; it is not a sandbox.
* Kernel and extension caches share a bounded, revision-aware Ring 0 LRU primitive that cleans itself up on `akasa.reset_state()`.
* `ExecutionFrame` tenant, origin, and baggage context survive parallel `run_parallel()` fan-out and asynchronous worker scheduling.
* Extensions and tests can execute VFS workflows purely in memory via `MemoryVFSTransaction`.
