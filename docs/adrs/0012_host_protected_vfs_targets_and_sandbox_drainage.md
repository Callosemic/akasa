# ADR 0012: Host-Protected VFS Targets, Import Shadowing Defense, and Pre-Reset Write Drainage

* **Status:** Accepted
* **Date:** 2026-10-30
* **Scope:** Akasa Micro-Kernel Architecture, VFS Pipeline, Security Substrate, & Test Sandbox Isolation

## Context
As the Akasa micro-kernel integrated with external host frameworks and automated test runners, two structural hazards emerged:
1. **Host Module Overwrite & Import Shadowing**: Host applications running on top of Akasa may place core launcher scripts, security stores, or entrypoint packages inside repository workspaces. If an untrusted agent or automated pipeline executes a VFS write, move, or deletion against these paths, it risks modifying host binaries or introducing import-shadowing variants (such as `<stem>/__init__.py`, `<stem>.pyc`, or `<stem>.cpython-*.so`).
2. **Stray VFS Writes Across Test Sandboxes**: During rapid unit test execution, test fixtures invoke `akasa.reset_state()` to clear caches and switch daemon working directories (`cwd`). However, if asynchronous writes remained in `_VFS_WRITE_QUEUE` while the working directory or tenant resolver reset, those writes could flush after the directory swap, writing files into adjacent test sandboxes.

## Decision
1. **Host-Protected Target Registry (`akasa.vfs.register_protected_vfs_targets`)**:
   - Implemented a dependency-injection registry accepting `(namespace, basename)` tuples or normalized paths.
   - Enforced checks across `execute_vfs_save()`, `execute_vfs_delete()`, `execute_vfs_move()`, and `execute_vfs_bulk()`, returning generic `403` error payloads (`_PROTECTED_WRITE_MESSAGE`) without leaking host filenames.
2. **Whole-Segment Matching & Import Shadowing Mitigations (`_protected_segment_match`)**:
   - Refuses mutations to the target module itself across path variants.
   - Blocks paths that would shadow the module during Python imports: package directories (`<stem>/__init__.py`), compiled extensions (`<stem>.cpython-*.so`), bytecode (`<stem>.pyc`), and `__pycache__` artifacts.
   - Enforces whole path segment matching, ensuring substrings do not accidentally collide with unrelated files.
3. **Explicit Write Drainage & Sandbox Quiescence (`drain_write_queue`, `reset_state`)**:
   - Factored out `drain_write_queue(timeout=5.0)` to flush all pending queue writes to disk with barrier guarantees.
   - Wired `drain_write_queue()` into `reset_state()` prior to `_akasa_context.reset()`, ensuring all writes commit under their originating working directory and volume mount table.
4. **Reserved Orchestration Template Namespaces (`akasa.context`)**:
   - Reserved `"sys"` in `_RESERVED_TEMPLATE_MACROS` and `"sys."` in `_RESERVED_TEMPLATE_PREFIXES` within template parameter extraction and variable interpolation.

## Consequences
* Host application entrypoints and core runtime modules cannot be modified, moved, deleted, or shadowed via VFS pipelines.
* Rejections adhere strictly to the Blind Substrate Mandate by using generic error responses.
* Test fixtures guarantee zero cross-sandbox filesystem contamination upon `reset_state()`.
* Orchestrator system parameters are preserved during template resolution.
