# 🌌 Akasa: a small Python kernel for local-first applications
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![PyPI](https://img.shields.io/pypi/v/akasa.svg)](https://pypi.org/project/akasa/)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue.svg)](pyproject.toml)

**Akasa supplies the parts of a local-first, multi-tenant Python application that are not the application itself: a virtual file system with a write queue, an event bus, a background job engine, pooled SQLite, scoped settings and secrets, sandboxed subprocesses, and authentication.**

Akasa was extracted from [inSetu](https://github.com/Callosemic/insetu), which runs on it every day. It knows nothing about its host's domain. A host plugs in through hooks, drivers and volume mounts, and declares its features as extensions: Flask blueprints whose routes, hooks and workers share one context bound to the active workspace.

The name is Sanskrit for *ether* or *space*.

| | |
| :--- | :--- |
| **Status** | Beta (`0.x`). The public API can still change between minor versions, so pin a minor range (`akasa>=0.6,<0.7`). |
| **Dependencies** | Python 3.10+, Flask, `cryptography` |
| **Used by** | [inSetu](https://github.com/Callosemic/insetu) |
| **Install** | `pip install akasa` |

## Engineering highlights

* **Layering is enforced, not documented.** Modules sit in five rings and may import only from their own ring or lower ones. An AST fitness rule (`KERNEL_RING_DAG_MANDATE`) fails the build on an upward import. Flask is confined to the top ring, so rings 0 to 3 run headless in CLIs, workers and tests.
* **One context per tenant.** Databases, caches, settings and file resolution are all scoped to a `workspace_id`, which travels with each call in an `ExecutionFrame` (HTTP, IPC, worker or CLI). One daemon can serve several isolated workspaces.
* **Writes that cannot interleave.** VFS writes go through a per-workspace commit queue. `sync_vfs_barrier()` waits for it to drain, and hosts can register files the VFS refuses to modify.
* **Authentication that fails closed.** A request reaching the daemon through any proxy can never be treated as localhost, whatever its headers claim. Host validation blocks DNS rebinding. The credential store is standard-library only, so a host's recovery server can load it even when the rest of akasa cannot import.
* **A test harness contract.** `akasa.reset_state()` quiesces workers, drains the write queue and resets every ring, so each test runs in its own sandbox without leaking files or threads.

## ✨ Core mechanics

1. **Volume Router (VFS):** an asynchronous virtual file system. A mount table maps sandboxed repositories and virtual domains to unified URIs (`vfs://`, `ctx://`). Writes go through a per-workspace commit queue; `sync_vfs_barrier()` waits for it to drain. `register_protected_vfs_targets()` lets a host name files that VFS writes must refuse, matched by whole path segments.
2. **Stateless multi-tenancy:** every database, cache, setting and file resolution is scoped to the active `workspace_id`, carried by an `ExecutionFrame`.
3. **Event bus:** a synchronous and asynchronous `HookRegistry` with authorization per extension and workspace. Disabled extensions never receive events.
4. **Metronome (worker engine):** a SQLite-backed job ledger (`jobs` and `immediate_jobs`) for recurring, one-shot and DAG-chained tasks, with progress telemetry, off the request thread.
5. **SQLite WAL connection pooling:** thread-local, auto-evicting connections tuned for Write-Ahead Logging, with declarative per-extension schemas and automatic column migrations.
6. **Scoped settings & secrets:** a schema-driven `SettingsManager` with `daemon`, `workspace`, `repo` and `secure` scopes. Secure values are Fernet-encrypted in `secrets.json` under a master key kept outside the workspace.
7. **Capability registry:** every extension route is also a named capability (`<extension>.<action>`) with request and response `TypedDict` schemas. The same handler can be called over HTTP or in-process with `ctx.call()`, and hosts can export the registry as OpenAPI or MCP tool descriptions.
8. **Sandboxed subprocesses:** `execute_binary()` resolves binaries across PATH, virtual environments and module fallbacks, and never passes the session token to children. A command with a timeout runs in its own process group, and the timeout kills the whole tree, so grandchildren cannot keep ports or locks. Shell commands (`shell=True`) reach the shell exactly as written.
9. **Authentication & peer trust:** a session token gate, a three-route token exchange (`/auth/bootstrap`), fail-closed proxy detection and Host validation. See [Security model](#-security-model).

## 🏗️ Architecture mandate

Akasa follows the **Blind Substrate Mandate**:
* **Zero domain logic:** Akasa does not parse repositories, compile LLM context or understand Git diffs. It only provides the tools to store and route them.
* **Inversion of control:** all application behavior is injected by the host through `@hooks.on()` subscriptions, driver registrations and volume mounts.

### The ring structure

Kernel modules are layered in rings. A module may import from its own ring or any lower ring, never upward. The AST fitness suite enforces this (`KERNEL_RING_DAG_MANDATE`), so the layering cannot drift silently.

| Ring | Modules | Responsibility |
| :--- | :--- | :--- |
| **0** | `types`, `uri`, `context`, `merge` | Pure standard-library primitives: execution frames, URI parsing, atomic writes, template interpolation, 3-way text merge. |
| **1** | `events`, `hooks` | The event bus, hook authorization and the server-sent events (SSE) channel. |
| **2** | `db`, `vfs`, `state`, `utils` | Persistence: SQLite pooling and migrations, the VFS and write queue, workspace configuration. |
| **3** | `process`, `workers`, `sync` | Execution: sandboxed subprocesses, the Metronome job engine, state heartbeats. |
| **4** | `auth`, `extension`, `akasa/__init__.py` | Transport and SDK: Flask wiring, authentication, the `AkasaExtension` blueprint and the two-phase bootloader. |

Flask is confined to Ring 4 (`KERNEL_TRANSPORT_ISOLATION`). Within Ring 4, `auth/credentials.py` imports nothing but the standard library (`CREDENTIALS_STDLIB_ONLY`), so recovery servers can load it by file path.

## 🚀 Installation

Akasa is installed as a dependency of your host application. It requires **Python 3.10+**.

```bash
pip install akasa
```

## ⚡ Quick start

A host application configures the kernel, declares extensions and wires the Flask transport:

```python
from flask import Flask

import akasa
from akasa.extension import AkasaExtension

# Phase A: process physics (no Flask needed; safe in CLIs and workers)
akasa.configure(control_dir=".myapp", extension_namespaces=["myapp.extensions."])

# An extension: routes, hooks and workers share one tenant-scoped context
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

The route is served at `/api/<workspace_id>/notes/greet` and registered as the capability `notes.greet`. Any other extension can call it in-process with `ctx.call("notes.greet")`.

## 🔐 Security model

Akasa assumes a **single trusted user per daemon**. An authenticated session holds the daemon's full authority, so treat the session token like a shell login.

* **Session token:** every request except `/auth/bootstrap` and any routes you declare public must carry the token (header `X-Akasa-Token` by default). The token is durable: it is created on first use in `<control_dir>/auth/session_token` (directory `0700`, file `0600`), so devices stay signed in across restarts and a recovery server reading the same file accepts them too. `AKASA_BOOT_TOKEN` overrides it.
* **Token exchange (`/auth/bootstrap`):** three routes, tried in order:
  1. **Localhost:** an unproxied loopback connection that names a loopback Host (and, when present, a loopback Origin and Referer).
  2. **Identity hook:** host handlers on `execute_identity_handshake` receive the classified peer and may vouch for it (inSetu uses this for Tailscale identity).
  3. **Static token:** the `auth_token` from the workspace configuration.
* **Fail-closed proxy detection:** `classify_peer()` treats any forwarding header (`Forwarded`, `X-Forwarded-*`, `X-Real-IP`, or one the host registers) as proxied, so a proxy can never inherit localhost trust. Only the rightmost `X-Forwarded-For` hop is trusted as the client address. Host code must not read these headers itself (`FORWARDED_HEADER_MEMBRANE`).
* **Host validation:** requests whose Host is not loopback, a registered trusted suffix or an explicitly allowed hostname are rejected with `421 Misdirected Request`, which blocks DNS-rebinding attacks.
* **Protected files:** files a host registers with `register_protected_vfs_targets()` cannot be modified through the VFS.
* **Child processes:** `execute_binary()` strips the session token from every child's environment.
* **Secrets:** secure settings are encrypted at rest with Fernet. The master key lives in `~/<control_dir>/master_key.txt`, outside the workspace, and can be rotated.

## 🧪 Development & testing

```bash
git clone https://github.com/Callosemic/akasa.git
cd akasa
pip install -e .

python tests/run_unit_tests.py      # unit suite
python tests/fitness_functions.py   # architectural fitness rules (ring DAG, transport isolation, ...)
```

The fitness suite checks the architecture statically: ring layering, Flask confinement, SQLite access only through `db.py`, VFS writes only through the write pipeline, a standard-library-only credential store, zero host-domain leakage, and more. CI runs both suites together with sutram and inSetu on Python 3.10 through 3.12.

`akasa.reset_state()` is the test-harness contract: it quiesces workers, drains the VFS queue and resets every ring's state, so each test can run in its own sandbox.

The [`docs/`](docs/) folder holds the engineering standards and the kernel API and IoC contracts.

## 📄 License

Licensed under the **Apache License, Version 2.0**. See the [LICENSE](LICENSE) and [NOTICE](NOTICE) files for details.
