# 🌌 Akasa: Application Kernel & Volume Router

**Akasa** (Sanskrit for *Ether / Substrate / Space*) is a local-first, event-sourced Python micro-kernel. 

Extracted from the [inSetu Developer OS](https://github.com/Callosemic/insetu), Akasa acts as the foundational substrate for multi-tenant, offline-first Python applications. It is strictly domain-agnostic: it knows nothing about your application's business logic, acting purely as a high-performance routing and synchronization engine.

## ✨ Core Mechanics

1. **The Volume Router (VFS):** An asynchronous, non-blocking Virtual File System. It abstracts physical disk I/O behind a strict Mount Table, allowing host applications to mount sandboxed repositories and virtual domains to unified URIs (e.g., `vfs://` or `ctx://`).
2. **Stateless Multi-Tenancy:** All databases, caches, and file resolutions are scoped to an active `workspace_id`, allowing a single daemon process to serve multiple isolated tenant environments safely.
3. **The Typed Event Bus:** A synchronous/asynchronous `HookRegistry` enforcing strict authorization boundaries for inter-module communication.
4. **The Metronome (Worker Engine):** A background SQLite-backed job ledger (`jobs` and `immediate_jobs`) providing distributed, dependency-driven task execution and UI telemetry without blocking the main event loop.
5. **SQLite WAL Connection Pooling:** Thread-local, auto-evicting database connection pooling explicitly optimized for Write-Ahead Logging concurrency.

## 🏗️ Architecture Mandate

Akasa adheres to the **Blind Substrate Mandate**.
* **Zero Domain Logic:** Akasa does not parse target repositories, compile RAG contexts, or understand Git diffs. It only provides the tools to store and route them.
* **Inversion of Control:** All application-specific behavior is injected into Akasa by the host application via `@hooks.on()` subscriptions, Driver registrations, and Volume mounts.

## 🚀 Installation

Akasa is designed to be installed as a foundational dependency for your host application.

```bash
pip install akasa

```
