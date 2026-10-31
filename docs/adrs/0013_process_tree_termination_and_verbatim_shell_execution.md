# ADR 0013: Process-Tree Termination on Timeout and Verbatim Shell Command Execution

* **Status:** Accepted
* **Date:** 2026-10-30
* **Scope:** Akasa Micro-Kernel Architecture, Process Sandboxing, OS Session Governance, & Substrate Resilience

## Context
When running volume-sandboxed processes inside `ProcessManager` or invoking external tools via `execute_binary()`, two critical execution edge cases created runtime friction:
1. **Orphaned Descendant Processes on Timeout**: Standard library `subprocess.run(timeout=...)` only signals the immediate child process when a deadline expires. If the child process is a shell script, test runner, compiler, or build orchestrator that spawned background grandchildren, killing the direct parent abandons the grandchildren. These runaway descendant processes continue running in the background, consuming CPU, holding SQLite file locks, and blocking TCP ports.
2. **Compound Shell Command Mangling**: `execute_binary()` performed binary-name resolution heuristics by inspecting `cmd[0]` and attempting system `PATH`, virtual environment, and `sys.executable -m <module>` lookups. When callers passed compound shell commands with `shell=True` (such as `"build && test"`), the resolution logic evaluated `"build && test"` as a binary name, transforming it into `[sys.executable, "-m", "build && test"]` and causing immediate syntax failures.

## Decision
1. **Isolated Session & Process Group Leadership (`_run_with_tree_timeout`)**:
   - Timed process executions invoke `_run_with_tree_timeout()`, starting the process as the leader of a new process group or session (`start_new_session=True` via `setsid` on POSIX, and `CREATE_NEW_PROCESS_GROUP` creation flag on Windows).
2. **Whole-Tree Process Termination (`_kill_process_tree`)**:
   - Upon `subprocess.TimeoutExpired` or any unhandled `BaseException`, the entire process tree is terminated atomically:
     - On POSIX: `os.killpg(proc.pid, signal.SIGKILL)`.
     - On Windows: `taskkill /F /T /PID <pid>`.
   - Includes fallback to `proc.kill()` and catches `ProcessLookupError`, `PermissionError`, and `OSError`.
   - Incorporates a 5-second drain window on pipe streams (`proc.communicate(timeout=5)`) to harvest partial output without hanging indefinitely if an escaping descendant holds open file descriptors.
3. **Verbatim Shell Command Pass-Through**:
   - When `kwargs.get("shell")` is truthy, `execute_binary()` bypasses binary-name token parsing, venv path lookups, and Python module fallbacks. Sequences are converted via `shlex.join()` and strings are passed directly to the system shell.

## Consequences
* Sandboxed processes that time out cannot leak background descendant processes, preventing zombie port consumption and database lock contention.
* Complex shell pipelines and compound commands execute reliably without unexpected CLI rewrites.
* Subprocess timeout semantics remain consistent with `subprocess.run()`, returning partial output inside `TimeoutExpired` exceptions.
