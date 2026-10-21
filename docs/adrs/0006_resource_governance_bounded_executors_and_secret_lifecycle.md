# ADR 0006: Kernel Resource Governance, Bounded Thread Pools, and Secret Masking Lifecycle

* **Status:** Accepted
* **Date:** 2026-10-19
* **Scope:** Akasa Micro-Kernel Architecture, Memory Management, Worker Concurrency, & Security Subsystem

## Context
As concurrent workloads, large workspace compilations, and extension interactions scaled within the single daemon process, four key resource constraints emerged:
1. **Unbounded Thread Creation & OS Resource Contention:** `HookRegistry.emit_background` spawned an unmanaged `threading.Thread` per async invocation, and parallel operations lacked a unified, bounded thread executor with automatic thread-local connection cleanup. Worker pool sizing (20 threads) produced thread contention under high load.
2. **Process Memory Footprint & Glibc Fragmentation:** High-throughput JSON configuration loading and DAG pipeline executions accumulated heap allocations. Python garbage collection alone failed to return unmapped arenas back to the Linux host kernel, leading to persistent memory bloat.
3. **Secret Exposure in Settings Payloads:** Extension settings endpoints returned raw Fernet-decrypted values to client inspection layers, and form submissions risked overwriting secrets with display placeholders.
4. **VFS URI Ambiguity:** `VFSTransaction` permitted bare relative paths without canonical scheme prefixes (`scheme://`), bypassing strict volume boundary guarantees.

## Decision
1. **Kernel Memory Governance (`reclaim_process_memory`):** Introduced a centralized memory reclamation utility in `akasa.utils` executing Python `gc.collect()` and invoking Linux glibc `malloc_trim(0)` via `ctypes`. Memory reclamation is triggered automatically upon DAG pipeline completion, sequential job chain completion, and ephemeral artifact sweeps.
2. **Bounded Thread Pools & Deterministic Connection Eviction:**
   - Standardized `HookRegistry.emit_background` on a bounded 4-worker `ThreadPoolExecutor` (`Akasa-EventBus`).
   - Introduced `run_parallel()` in `akasa.workers` and `map_parallel()` in `JobManager`, backed by a bounded 4-worker `ThreadPoolExecutor` (`Akasa-IO`), ensuring `close_thread_connections()` executes deterministically on worker thread completion.
   - Reduced the Metronome worker thread pool from 20 to 8 workers (`Akasa-Worker`).
3. **Secret Masking & Master Key Rotation:**
   - Standardized secret masking using `MASKED_SECRET_SENTINEL = "••••••••"`. Settings queries default to masking encrypted fields (`mask_secrets=True`), and write operations (`set`, `update`) drop sentinel values to prevent overwrite.
   - Implemented `rotate_master_key` in `akasa.auth`, decrypting with the prior key, encrypting with the new Fernet key, updating all workspace `secrets.json` files, and persisting the master key with `0o600` permissions.
4. **Strict Canonical URI Enforcement:** Added runtime validation across `VFSTransaction.save`, `delete`, and `read`, rejecting paths lacking `://` with `ValueError` unless explicitly flagged as `is_absolute_artifact`.
5. **Zero-Copy Cache Footprint Optimization:**
   - Reduced SQLite connection `cache_size` from `-8000` to `-2000` (2 MB).
   - Replaced `copy.deepcopy` inside `@thread_safe_cache` with shallow copies to eliminate serialization bottlenecks on hot cache hits.
   - Reduced `_VFS_READ_CACHE` capacity from 32 MB to 8 MB with a 1 MB file threshold.

## Consequences
* Daemon process memory stays strictly bounded and releases freed memory back to the host operating system.
* Thread pool sizes are bounded across all subsystems, eliminating thread exhaustion.
* Extensions cannot leak thread-local SQLite handles during fan-out parallel processing.
* Encrypted credentials cannot be exposed to UI clients or accidentally clobbered by form updates.
* VFS mutations require unambiguous RFC 3986 URIs, preventing routing ambiguity.