# 🌌 Akasa: Application Kernel & Volume Router
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

**Akasa** (Sanskrit for *Ether / Substrate / Space*) is a local-first, event-sourced Python micro-kernel. 

Extracted from the [inSetu Developer OS](https://github.com/Callosemic/insetu), Akasa acts as the foundational substrate for multi-tenant, offline-first Python applications. It is strictly domain-agnostic: it knows nothing about your application's business logic, acting purely as a high-performance routing and synchronization engine.

> **Status:** beta (`0.x`). Akasa powers inSetu in daily use, but its public API can still change between minor versions. Pin an exact version in your host application.

## ✨ Core Mechanics

1. **The Volume Router (VFS):** An asynchronous, non-blocking Virtual File System. It abstracts physical disk I/O behind a strict Mount Table, allowing host applications to mount sandboxed repositories and virtual domains to unified URIs (e.g., `vfs://` or `ctx://`). Writes go through a per-workspace commit queue; `sync_vfs_barrier()` waits for it to drain.
2. **Stateless Multi-Tenancy:** All databases, caches, settings, and file resolutions are scoped to an active `workspace_id`, allowing a single daemon process to serve multiple isolated tenant environments safely. The active tenant travels with each call in an `ExecutionFrame` (HTTP, IPC, worker, or CLI).
3. **The Typed Event Bus:** A synchronous/asynchronous `HookRegistry` enforcing strict authorization boundaries for inter-module communication. Disabled extensions never receive events.
4. **The Metronome (Worker Engine):** A background SQLite-backed job ledger (`jobs` and `immediate_jobs`) providing recurring, one-shot, and DAG-chained task execution with progress telemetry, without blocking the request thread.
5. **SQLite WAL Connection Pooling:** Thread-local, auto-evicting database connection pooling explicitly optimized for Write-Ahead Logging concurrency, with declarative per-extension schemas and automatic column migrations.
6. **Scoped Settings & Secrets:** A schema-driven `SettingsManager` with `daemon`, `workspace`, `repo`, and `secure` scopes. Secure values are Fernet-encrypted in `secrets.json` under a master key kept outside the workspace.
7. **Capability Registry:** Every extension route is also registered as a named capability (`<extension>.<action>`) with its request/response `TypedDict` schemas, so the same handler can be called over HTTP or in-process via `ctx.call()`, and hosts can export the registry as OpenAPI or MCP tool descriptions.
8. **Authentication & Peer Trust:** A per-process session token gate, a three-route token exchange (`/auth/bootstrap`), fail-closed reverse-proxy detection, and Host validation against DNS rebinding. See [Security Model](#-security-model).

## 🏗️ Architecture Mandate

Akasa adheres to the **Blind Substrate Mandate**.
* **Zero Domain Logic:** Akasa does not parse target repositories, compile RAG contexts, or understand Git diffs. It only provides the tools to store and route them.
* **Inversion of Control:** All application-specific behavior is injected into Akasa by the host application via `@hooks.on()` subscriptions, Driver registrations, and Volume mounts.

### The Ring Structure

Kernel modules are layered in rings. A module may import from its own ring or any lower ring, never upward. The rule is enforced by the AST fitness suite (`KERNEL_RING_DAG_MANDATE`), so the layering cannot drift silently.

| Ring | Modules | Responsibility |
| :--- | :--- | :--- |
| **0** | `types`, `uri`, `context` | Pure standard-library primitives: execution frames, URI parsing, atomic writes, template interpolation. |
| **1** | `events`, `hooks` | The event bus, hook authorization, and the server-sent events (SSE) channel. |
| **2** | `db`, `vfs`, `state`, `utils` | Persistence: SQLite pooling and migrations, the VFS and write queue, workspace configuration. |
| **3** | `process`, `workers`, `sync` | Execution: sandboxed subprocesses, the Metronome job engine, state heartbeats. |
| **4** | `auth`, `extension`, `akasa/__init__.py` | Transport and SDK: Flask wiring, authentication, the `AkasaExtension` blueprint, and the two-phase bootloader. |

Flask is confined to Ring 4 (`KERNEL_TRANSPORT_ISOLATION`), so Rings 0 through 3 run headless in CLIs, workers, and tests.

## 🚀 Installation

Akasa is designed to be installed as a foundational dependency for your host application. It requires **Python 3.10+**.

```bash
pip install akasa
```

## ⚡ Quick Start

A host application configures the kernel, declares extensions, and wires the Flask transport:

```python
from flask import Flask

import akasa
from akasa.extension import AkasaExtension

# Phase A: process physics (no Flask needed; safe in CLIs and workers)
akasa.configure(control_dir=".myapp", extension_namespaces=["myapp.extensions."])

# An extension: routes, hooks, and workers share one tenant-scoped context
notes = AkasaExtension("notes", __name__, settings_schema=[
    {"id": "greeting", "label": "Greeting", "type": "text", "scope": "workspace", "default": "hello"},
])

@notes.route("greet", methods=["GET"], docstring="[sync] Returns the configured greeting.")
def greet(ctx):
    return {"workspace": ctx.workspace_id, "greeting": ctx.settings.get("greeting")}

@notes.on("note_saved")
def index_note(ctx, path=None, workspace_id=None, **kwargs):
    ctx.jobs.submit("reindex", path=path)

@notes.worker("reindex")
def reindex(ctx, path: str = "", job_id: str = None):
    ctx.jobs.update_progress(f"Indexed {path}")

# Phase B: wire the Flask transport (token gate, Host validation, DB teardown)
app = Flask(__name__)
akasa.init_app(app)
app.register_blueprint(notes.bp)

if __name__ == "__main__":
    akasa.boot()
    app.run(host="127.0.0.1", port=5005)
```

Extensions are opt-in per workspace. Enable `notes` in the workspace configuration (`.myapp/config.json`):

```json
{
    "extensions": ["notes"]
}
```

The route is served at `/api/<workspace_id>/notes/greet` and is registered as the capability `notes.greet`. Any other extension can call it in-process with `ctx.call("notes.greet")`.

## 🔐 Security Model

Akasa assumes a **single trusted user per daemon**. An authenticated session holds the daemon's full authority, so treat the session token like a shell login.

* **Session token:** Every request except `/auth/bootstrap` and any routes you declare public must carry the per-process token (header `X-Akasa-Token` by default). The token is generated at startup, or taken from `AKASA_BOOT_TOKEN`.
* **Token exchange (`/auth/bootstrap`):** Three routes, tried in order:
  1. **Localhost:** an unproxied loopback connection that names a loopback Host (and, when present, a loopback Origin and Referer).
  2. **Identity hook:** host-provided handlers on `execute_identity_handshake` receive the classified peer and may vouch for it (inSetu uses this for Tailscale identity).
  3. **Static token:** the `auth_token` from the workspace configuration.
* **Fail-closed proxy detection:** `classify_peer()` treats any forwarding header (`Forwarded`, `X-Forwarded-*`, `X-Real-IP`, or one the host registers) as proxied, so a proxy can never inherit localhost trust. Only the rightmost `X-Forwarded-For` hop is trusted as the client address. Host code must not read these headers itself (`FORWARDED_HEADER_MEMBRANE`).
* **Host validation:** Requests whose Host is not loopback, a registered trusted suffix, or an explicitly allowed hostname are rejected with `421 Misdirected Request`, which blocks DNS-rebinding attacks.
* **Secrets:** Secure settings are encrypted at rest with Fernet. The master key lives in `~/<control_dir>/master_key.txt`, outside the workspace, and can be rotated.

## 🧪 Development & Testing

```bash
git clone https://github.com/Callosemic/akasa.git
cd akasa
pip install -e .

python tests/run_unit_tests.py      # unit suite
python tests/fitness_functions.py   # architectural fitness rules (ring DAG, transport isolation, ...)
```

The fitness suite statically checks the architecture: ring layering, Flask confinement, SQLite access only through `db.py`, VFS writes only through the write pipeline, zero host-domain leakage, and more. CI runs both suites together with sutram and inSetu, on Python 3.10 through 3.12.

`akasa.reset_state()` is the test-harness contract: it quiesces workers, drains the VFS queue, and resets every ring's state, so each test can run in its own sandbox.

## 📄 License

Licensed under the **Apache License, Version 2.0**. See the [LICENSE](LICENSE) and [NOTICE](NOTICE) files for details.
