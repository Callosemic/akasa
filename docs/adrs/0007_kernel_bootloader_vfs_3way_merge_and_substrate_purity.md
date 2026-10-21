# ADR 0007: Kernel Bootloader Lifecycle, Causal 3-Way Merging, Substrate Purity, and Self-Healing Persistence

* **Status:** Accepted
* **Date:** 2026-10-21
* **Scope:** Akasa Micro-Kernel Architecture, Bootloader Protocol, VFS Pipeline, SQLite Engine, & Extension SDK

## Context
As the Akasa micro-kernel evolved into a standalone foundational package, several structural challenges emerged across the Tier 1 boundary:
1. **Bootstrapping Fragmentation**: Host applications lacked a standardized two-phase initialization protocol to configure headless physics, tune OS file descriptor and glibc arena limits, attach Flask transport security middleware, and manage test harness state resets.
2. **Concurrent File Write Conflicts & Read Latency**: While asynchronous VFS writing decoupled main threads from disk I/O, concurrent modifications from IDE watchdogs and background workers risked file clobbering without a deterministic 3-way text merger. Furthermore, reads dispatched immediately after writes risked reading stale cache data before disk commit completed.
3. **Database Corruption Data Loss**: Auto-quarantine isolated corrupted SQLite databases to `.corrupt_<timestamp>`, but lacked an automated salvaging mechanism to restore readable records back to clean databases.
4. **Substrate Domain Leakage**: Residual higher-tier application concepts (context manifests, hardcoded tunnel domain suffixes, and static SSE event names) persisted in `ExtensionContext`, `akasa.auth`, and `akasa.hooks`.

## Decision
1. **Kernel Bootloader & App Factory Protocol (`akasa/__init__.py`)**:
   - Implemented `configure()` for Phase A headless configuration (setting control directory, registering trusted namespaces, extension namespaces, core modules, and trusted host suffixes).
   - Added `tune_process_resources()` to raise `RLIMIT_NOFILE` to 65,536, cap `MALLOC_ARENA_MAX=2`, and restrict thread stack sizes to 1 MB.
   - Implemented `init_app()` for Phase B transport wiring, attaching `ForceHTTPSProxyFix`, enforcing token authentication gates, registering per-request SQLite connection cleanup on `@app.teardown_appcontext`, injecting browser anti-caching headers, and providing an optional SSE streaming route.
   - Added `boot()` with Werkzeug reload guardrails and `reset_state()` for test harness isolation.
2. **Synchronous Pending-Write Overlay & OCC 3-Way Merging (`akasa.vfs`)**:
   - Introduced `_VFS_PENDING_WRITES` memory overlay with monotonic sequencing (`_stage_pending_write`, `_clear_pending_write`). `VFSTransaction.read()` inspects this overlay before consulting disk or LRU caches, guaranteeing immediate Read-Your-Writes consistency.
   - Implemented `_ANCESTOR_CACHE` (ring buffer storing up to 12 historical revisions per file, capped at 1 MB each) alongside `compute_vfs_sha256()`, `record_vfs_ancestor()`, and `lookup_vfs_ancestor()`.
   - Introduced `three_way_merge_text()` featuring a Zombie-ACK detector (`_is_subsequence_edit`) to detect and accept consecutive edits from the same stream, paired with a pure-Python hunk merger for concurrent, non-overlapping modifications.
   - Added SHA-256 `base_hash` causal receipts to all `vfs_mutated` save events.
3. **Self-Healing SQLite Recovery (`akasa.db`)**:
   - Implemented `recover_corrupt_databases()` to automatically attach to quarantined `*.db.corrupt_*` database files via SQLite `ATTACH DATABASE`, salvaging undamaged rows using `INSERT OR IGNORE` into fresh schemas before the files are cleaned up.
   - Added `migrate_legacy_sqlite_paths()` for updating path substrings across tenant tables.
   - Added runtime schema checks in `get_connection()` to apply declarative schemas on lazily opened database connections.
4. **Substrate Purity Hardening (`akasa.extension`, `akasa.auth`, `akasa.hooks`)**:
   - Purged domain-specific manifest methods (`manifest`, `get_manifest_files`, `save_manifest`) from `ExtensionContext`.
   - Replaced hardcoded `.ts.net` domain checks in `bootstrap()` with `get_trusted_host_suffixes()` and `is_trusted_origin` hook delegation.
   - Replaced static SSE signal checks in `HookRegistry` with dynamic registration via `register_sse_passthrough()`.
   - Scoped daemon settings in `SettingsManager` to `<control_dir>/host.json` to keep `<control_dir>/system.json` dedicated to workspace routing and spatial physics.
5. **Universal Subprocess Execution & Worker Guardrails (`akasa.process`, `akasa.workers`)**:
   - Added `execute_binary()` resolving executable binaries across system `PATH`, virtual environment directories, and fallback Python CLI modules (`sys.executable -m`).
   - Added re-entrancy protection in `run_parallel()` to prevent deadlock when called from within an existing `Akasa-IO` thread.
   - Added query-response hook dispatchers (`emit_first`, `emit_merge_dict`, `emit_merge_list`) and `@ext.on()` decorator with automatic context hydration.

## Consequences
* The kernel provides a standardized application factory pattern for host integration and testing.
* Concurrent file writes merge safely without data loss, and reads are instantaneously consistent across threads.
* SQLite corruption events salvage recoverable records automatically.
* The kernel substrate is pure Tier 1, free of application-level manifest assumptions or hardcoded proxy domains.