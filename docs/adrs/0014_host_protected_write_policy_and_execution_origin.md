# ADR 0014: Host Policy for Protected Writes and Execution Origin

* **Status:** Accepted
* **Date:** 2026-11-03
* **Scope:** Akasa VFS, Execution Frames, Workers, Hooks

## Context
ADR 0012 lets a host register protected VFS targets, and the kernel refuses every mutation that touches one. That made those files impossible to change safely from inside the host, a person's own edits included. The host needs to decide instead:
- hold an automated change for a person to confirm;
- let a person's own edit through after checks;
- write a change the person confirmed.

To decide, the host has to know who started a call. The execution frame recorded the transport (HTTP, IPC, WORKER, CLI), but not whether an HTTP request came from a person or from a machine client. The answer was also lost when work moved to a worker job or a background hook.

## Decision
1. **Protected write handler.** `set_protected_write_handler(handler)`.
   - Without a handler, protected mutations are still refused (ADR 0012 behaviour).
   - With one, every save, delete, move and bulk item that touches a protected target is handed to the handler as `{action, workspace_id, filepath, dest_path, content, data, origin}`. The handler's `(body, status)` is returned to the caller, and nothing is written.
2. **Confirmed protected write.** `execute_confirmed_protected_write(workspace_id, filepath, content=None, data=None)`.
   - Lifts the protected-target check for that one call only, through a context variable that is reset when the call returns. Every other guard still applies.
   - Hosts call it only from their human confirmation path.
4. **Check and transactions (added before release).**
   - `is_protected_vfs_target(filepath, workspace_id=None, is_absolute_artifact=False)` is the same check the guard uses, so hosts can tell a person ahead of time that a change will be held.
   - `VFSTransaction` never buffers a protected mutation. It hands it to the handler at once and returns the handler's answer, so a caller inside a transaction sees it.
3. **Execution origin.**
   - `ExecutionFrame` gains `origin`, read with `get_current_origin()`.
   - An explicit origin wins. A nested frame keeps its caller's origin. A frame with no caller takes its transport's default: `HTTP` → `ui`, `WORKER` → `worker`, `CLI` → `cli`, `IPC` → `internal`.
   - Jobs carry the submitter's origin in `_origin`, and background hooks run with the emitter's origin.
   - Hosts set specific origins (for example `mcp`, `routine`, `agent`) where they dispatch.

## Consequences
* The kernel stays host-neutral. It knows protected targets, origins and one bypass, and names no host files or policies.
* Hosts can tell a person's request from automated work without guessing from transports, and the distinction survives worker jobs and background hooks.
* An origin is a label set by the host's own dispatchers, not an authentication. Code running in the process can set any origin or call the confirmed write. This keeps the host's own surfaces honest; it is not a sandbox.
* A protected mutation inside a `VFSTransaction` goes to the handler immediately. It writes nothing, so the transaction's atomicity is unchanged, but it is not withdrawn if the transaction later fails; the host keeps it (for example as a pending change a person can discard).
