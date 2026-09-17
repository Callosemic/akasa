# ADR 0001: Domain-Agnostic Inversion of Control & Process Sandboxing

* **Status:** Accepted
* **Date:** 2026-09-17
* **Scope:** Akasa Kernel Architecture & Extension SDK

## Context
Initial iterations of the Akasa kernel contained domain-specific assumptions, including hardcoded compiler task names, Tailscale identity headers, macro parsing utilities, and direct subprocess execution without volume isolation.

## Decision
1. **Inversion of Control (IoC):** Domain-specific decision-making is delegated to host applications or Tier 2/3 extensions via event bus hooks (`execute_identity_handshake`, `evaluate_blocking_task`, `request_vfs_ignore_dirs`).
2. **Process Sandboxing (`ProcessManager`):** All subprocess operations must be executed through `ProcessManager` anchored to a specific VFS volume mount, preventing path traversal breakout during external tool execution.
3. **Async VFS Persistence:** All disk mutations (including settings and garbage collection) must route through the non-blocking VFS commit queue.

## Consequences
* Tier 1 substrate remains strictly domain-blind and reusable across diverse host applications.
* Process executions are isolated to explicitly mounted workspace volumes.