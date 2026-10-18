# 🌌 Akasa Micro-Kernel Documentation Index

This directory contains the core architectural specifications, integration contracts, and engineering guardrails for **Akasa**, the domain-agnostic Tier 1 application kernel and volume router.

---

## 📚 Core Documentation
* **[01 Engineering Standards & Guardrails](./01_engineering_standards.md)**: Baseline rules for substrate purity, stateless multi-tenancy, VFS write-pipeline mechanics, and event bus authorization.

---
## 🏛️ Architectural Decision Records (ADRs)
* **[ADR 0001: Domain-Agnostic Inversion of Control & Process Sandboxing](./adrs/0001_domain_agnostic_ioc_and_process_sandboxing.md)**
* **[ADR 0002: Thread-Safe Caching, SSE Event Bus, and Non-Blocking VFS Barriers](./adrs/0002_thread_safe_caching_sse_bus_and_vfs_barriers.md)**
* **[ADR 0003: OpenAPI Reflection, DAG Step Orchestration, and System Error Hooks](./adrs/0003_openapi_reflection_dag_orchestration_and_error_hooks.md)**
* **[ADR 0004: Asynchronous VFS Bulk Mutation Batching and Transaction Consolidation](./adrs/0004_vfs_bulk_mutation_batching_and_atomic_consolidation.md)**
* **[ADR 0005: Parallel DAG Orchestration, In-Memory VFS Read Caching, and Inter-Workspace Routing](./adrs/0005_parallel_dag_orchestration_vfs_ram_caching_and_inter_workspace_routing.md)**