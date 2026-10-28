# 🌌 Akasa Micro-Kernel Documentation Index

This directory contains the core architectural specifications, integration contracts, and engineering guardrails for **Akasa**, the domain-agnostic Tier 1 application kernel and volume router.

---
## 📚 Core Documentation
* **[01 Engineering Standards & Guardrails](./01_engineering_standards.md)**: Baseline rules for substrate purity, stateless multi-tenancy, VFS write-pipeline mechanics, and event bus authorization.
* **[02 Kernel API & IoC Contracts](./02_kernel_api_and_ioc_contracts.md)**: Definitive external interface specification covering Host DI bootstrapping, Event Bus IoC hooks, `AkasaExtension`, `ExtensionContext`, VFS drivers, and worker orchestration.

---
## 🏛️ Architectural Decision Records (ADRs)
* **[ADR 0001: Domain-Agnostic Inversion of Control & Process Sandboxing](./adrs/0001_domain_agnostic_ioc_and_process_sandboxing.md)**
* **[ADR 0002: Thread-Safe Caching, SSE Event Bus, and Non-Blocking VFS Barriers](./adrs/0002_thread_safe_caching_sse_bus_and_vfs_barriers.md)**
* **[ADR 0003: OpenAPI Reflection, DAG Step Orchestration, and System Error Hooks](./adrs/0003_openapi_reflection_dag_orchestration_and_error_hooks.md)**
* **[ADR 0004: Asynchronous VFS Bulk Mutation Batching and Transaction Consolidation](./adrs/0004_vfs_bulk_mutation_batching_and_atomic_consolidation.md)**
* **[ADR 0005: Parallel DAG Orchestration, In-Memory VFS Read Caching, and Inter-Workspace Routing](./adrs/0005_parallel_dag_orchestration_vfs_ram_caching_and_inter_workspace_routing.md)**
* **[ADR 0006: Kernel Resource Governance, Bounded Thread Pools, and Secret Masking Lifecycle](./adrs/0006_resource_governance_bounded_executors_and_secret_lifecycle.md)**
* **[ADR 0007: Kernel Bootloader Lifecycle, Causal 3-Way Merging, Substrate Purity, and Self-Healing Persistence](./adrs/0007_kernel_bootloader_vfs_3way_merge_and_substrate_purity.md)**
* **[ADR 0008: Micro-Batched VFS Commits, Dual-Pool Worker Scheduling, Signature-Driven Type Validation, and Zero-Copy IPC Gateway](./adrs/0008_microbatched_vfs_dual_pool_workers_and_ipc_gateway.md)**
* **[ADR 0009: 5-Ring Strict Acyclic Kernel DAG, ExecutionFrame ContextVars, Inter-Process State Locking, and Zero-Copy Causal Config Caching](./adrs/0009_5ring_acyclic_dag_execution_frame_and_causal_state_caching.md)**