# ADR 0011: Durable Stdlib Credential Store, Lazy Boot Token Initialization, and Claim-Guarded Worker Settlement

* **Status:** Accepted
* **Date:** 2026-10-29
* **Scope:** Akasa Micro-Kernel Architecture, Authentication Subsystem, Worker Metronome, & Host Interoperability

## Context
As the Akasa micro-kernel integrated more deeply with host applications and external tooling (such as out-of-band recovery servers and debounced worker pipelines), three architectural constraints surfaced:
1. **Host Lifeboat Interoperability & Import Coupling:** External diagnostic and recovery servers needed to authenticate incoming requests against the daemon's session token. However, importing `akasa` or `akasa.auth` pulled in `Flask`, `cryptography`, and kernel state initialization, causing recovery utilities to fail when the core kernel environment was in a degraded state.
2. **Eager Import Side-Effects on Disk:** Importing `akasa.auth` eagerly generated session tokens and bound environment variables at module import time, violating the principle that library imports must produce zero filesystem side-effects.
3. **Worker Concurrency Race Conditions on Re-Submitted Jobs:** In `_execute_job()`, when a running job finished executing, it issued an unconditional `DELETE FROM jobs WHERE id=?` (for one-shots) or `UPDATE jobs SET status='pending' WHERE id=?` (for recurring jobs). If a job re-submitted itself during execution (such as a debounced file compile updating arguments and resetting status to `pending`) or was paused, the concluding worker thread would clobber the newer state or delete the re-submitted job entirely.

## Decision
1. **Isolated Stdlib Credential Store (`akasa/auth/credentials.py`)**:
   - Implemented an isolated, pure standard-library credential engine under `<control_dir>/auth/` (`0o700` directory, `0o600` file permissions) with zero dependencies on `Flask`, `cryptography`, or internal `akasa` modules.
   - Enforced standard-library purity via the `CREDENTIALS_STDLIB_ONLY` AST fitness rule in `tests/fitness/rules_python.py`.
   - Guaranteed race-safe token creation using POSIX `O_CREAT | O_EXCL` flags, atomic temporary file replacement (`tempfile.mkstemp`, `os.fchmod`, `os.fsync`, `os.replace`), and constant-time token verification (`hmac.compare_digest`).
2. **Lazy Boot Token Resolution (`get_boot_token`)**:
   - Deferred session token resolution until first access via `get_boot_token()`, eliminating filesystem writes during module import.
   - Guarded token initialization across threads with `_BOOT_TOKEN_LOCK`, giving precedence to explicit `AKASA_BOOT_TOKEN` environment variables, falling back to `<control_dir>/auth/session_token`, and degrading gracefully to in-memory tokens on read-only filesystems.
   - Preserved backward compatibility for `from akasa.auth import BOOT_TOKEN` and `akasa.BOOT_TOKEN` via module-level `__getattr__(name)`.
3. **Claim-Guarded Worker Settlement**:
   - Constrained post-execution rescheduling and deletion queries in `_execute_job()` strictly to `WHERE id=? AND status='running'`.
   - If a job was updated to `pending` with new parameters while running (e.g. via `submit_one_shot_job`), or transitioned to `paused`, the executing worker thread leaves the newer state intact.

## Consequences
* Recovery servers can load and verify session tokens directly by file path without importing `akasa` or third-party packages.
* Importing `akasa` produces zero side-effects on disk.
* Debounced tasks and job pause controls execute safely without state clobbering or dropped background workloads.
