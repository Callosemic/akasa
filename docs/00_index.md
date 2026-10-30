# 🌌 Akasa Micro-Kernel Documentation Index

This directory contains the core architectural specifications, integration contracts, and engineering guardrails for **Akasa**, the domain-agnostic Tier 1 application kernel and volume router.

---
## 📚 Core Documentation
* **[01 Engineering Standards & Guardrails](./01_engineering_standards.md)**: Baseline rules for substrate purity, stateless multi-tenancy, VFS write-pipeline mechanics, and event bus authorization.
* **[02 Kernel API & IoC Contracts](./02_kernel_api_and_ioc_contracts.md)**: Definitive external interface specification covering Host DI bootstrapping, Event Bus IoC hooks, `AkasaExtension`, `ExtensionContext`, VFS drivers, and worker orchestration.

---
## 🏛️ Architectural Decision Records (ADRs)

### Active Specifications
* **[ADR 0006: Kernel Resource Governance, Bounded Thread Pools, and Secret Masking Lifecycle](./adrs/0006_resource_governance_bounded_executors_and_secret_lifecycle.md)**
* **[ADR 0007: Kernel Bootloader Lifecycle, Causal 3-Way Merging, Substrate Purity, and Self-Healing Persistence](./adrs/0007_kernel_bootloader_vfs_3way_merge_and_substrate_purity.md)**
* **[ADR 0008: Micro-Batched VFS Commits, Dual-Pool Worker Scheduling, Signature-Driven Type Validation, and Zero-Copy IPC Gateway](./adrs/0008_microbatched_vfs_dual_pool_workers_and_ipc_gateway.md)**
* **[ADR 0009: 5-Ring Strict Acyclic Kernel DAG, ExecutionFrame ContextVars, Inter-Process State Locking, and Zero-Copy Causal Config Caching](./adrs/0009_5ring_acyclic_dag_execution_frame_and_causal_state_caching.md)**
* **[ADR 0010: Fail-Closed Peer Classification, DNS-Rebinding Host Gate, Topological DAG Wavefronts, and Test Sandbox Quiescing](./adrs/0010_fail_closed_peer_trust_dns_rebinding_gate_and_dag_wavefronts.md)**
* **[ADR 0011: Durable Stdlib Credential Store, Lazy Boot Token Initialization, and Claim-Guarded Worker Settlement](./adrs/0011_durable_credential_store_lazy_tokens_and_guarded_job_settlement.md)**
* **[ADR 0012: Host-Protected VFS Targets, Import Shadowing Defense, and Pre-Reset Write Drainage](./adrs/0012_host_protected_vfs_targets_and_sandbox_drainage.md)**

### Superseded Records
* **[ADR 0003: OpenAPI Reflection, DAG Step Orchestration, and System Error Hooks](./adrs/superseded/0003_openapi_reflection_dag_orchestration_and_error_hooks.md)** *(Superseded by ADR 0010)*
* **[ADR 0005: Parallel DAG Orchestration, In-Memory VFS Read Caching, and Inter-Workspace Routing](./adrs/superseded/0005_parallel_dag_orchestration_vfs_ram_caching_and_inter_workspace_routing.md)** *(Superseded by ADR 0006, ADR 0008, and ADR 0010)*

### Archived Baseline
* **[ADR 0001: Domain-Agnostic Inversion of Control & Process Sandboxing](./adrs/archived/0001_domain_agnostic_ioc_and_process_sandboxing.md)** *(Archived baseline)*
* **[ADR 0002: Thread-Safe Caching, SSE Event Bus, and Non-Blocking VFS Barriers](./adrs/archived/0002_thread_safe_caching_sse_bus_and_vfs_barriers.md)** *(Archived baseline)*
* **[ADR 0004: Asynchronous VFS Bulk Mutation Batching and Transaction Consolidation](./adrs/archived/0004_vfs_bulk_mutation_batching_and_atomic_consolidation.md)** *(Archived baseline)*