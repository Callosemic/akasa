# ADR 0004: Asynchronous VFS Bulk Mutation Batching and Transaction Consolidation

* **Status:** Accepted
* **Date:** 2026-10-15
* **Scope:** Akasa Micro-Kernel Architecture, VFS Pipeline, & Event Bus

## Context
When multi-file operations (such as code generation, workspace migrations, or multi-target refactors) executed inside `VFSTransaction`, each buffered mutation was dispatched sequentially as an independent `execute_vfs_save` item on the `_VFS_WRITE_QUEUE`. This introduced several structural bottlenecks:
1. High queue contention on `_VFS_WRITE_QUEUE` during large batch operations.
2. Fragmented SQLite transactions on the `vfs_event_log` table, executing individual `INSERT OR REPLACE` queries rather than batch commits.
3. Event bus flooding from multiple rapid `vfs_mutated` event emissions, triggering redundant downstream re-indexing sweeps and telemetry broadcasts.

## Decision
1. **Asynchronous Bulk Queue Action (`execute_vfs_bulk`):** Introduced a first-class `bulk` action to `_VFS_WRITE_QUEUE` and `_vfs_commit_worker`, accepting an array of mutation dictionaries (`operations`).
2. **Batched Ledger Persistence (`executemany`):** Background worker accumulates deleted and moved file records, committing them to `vfs_event_log` in a single SQLite `executemany()` operation.
3. **Consolidated Mutation Telemetry:** Grouped mutations are emitted in a single `hooks.emit('vfs_mutated', workspace_id=workspace_id, mutations=emitted_mutations)` notification.
4. **Transparent Context Aggregation:** Enhanced `VFSTransaction.__exit__` to automatically evaluate buffer length. Isolated 1-item buffers fall back to `execute_vfs_save`, while buffers containing multiple items are packaged and dispatched as a unified `execute_vfs_bulk` transaction.

## Consequences
* Reduces queue contention and thread context switching during multi-file operations.
* Minimizes SQLite WAL write lock contention via batched ledger updates.
* Prevents event bus cascading and re-index churn across listeners.
* Maintains backward compatibility for single-file saves.