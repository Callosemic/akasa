# ADR 0002: Thread-Safe Caching, SSE Event Bus, and Non-Blocking VFS Barriers

* **Status:** Accepted
* **Date:** 2026-09-21
* **Scope:** Akasa Micro-Kernel Architecture & Extension SDK

## Context
As concurrent tenant workloads increased, direct cache dict mutation (`_MUTATED_CONFIG_CACHE`) led to potential race conditions across worker threads. In addition, VFS write barrier synchronization relied on CPU-bound sleep polling loops, and real-time frontend notifications lacked a lightweight, thread-safe Server-Sent Events (SSE) broadcasting mechanism.

## Decision
1. **Thread-Safe Single-Flight TTL Cache:** Replaced manual cache dictionary clearing with `@thread_safe_cache` decorator implementing lock-free reads, per-key single-flight computation locks, and timestamped invalidation protection tied to `config_mutated` hooks.
2. **Global SSE Event Bus (`sse_bus`):** Introduced `akasa/events.py` featuring a thread-safe `EventBus` singleton to broadcast real-time telemetry, job progress, and topology resolution signals directly to connected HTTP clients.
3. **Queue Sentinel VFS Barriers:** Standardized VFS read-after-write barriers around queue-based event sentinels (`{"action": "barrier", "barrier_event": event}`), replacing polling loops with non-blocking `threading.Event.wait()`.
4. **SQLite Auto-Healing & Spatial DI Routing:** Implemented `PRAGMA quick_check(1)` auto-quarantine for corrupted database files, added worker-loop thread-local connection eviction upon corruption detection, and added dependency-injection interfaces (`register_trusted_namespace`, `set_workspace_router`) to `HookRegistry`.

## Consequences
* Eliminates cache stampedes and stale configuration reads across worker threads.
* Reduces CPU usage during synchronous VFS writes to zero idle overhead.
* Enables real-time UI telemetry without coupling Tier 1 kernel to WebSocket or domain-specific streaming frameworks.