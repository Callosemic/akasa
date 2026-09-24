# ADR 0003: OpenAPI Reflection, DAG Step Orchestration, and System Error Hooks

* **Status:** Accepted
* **Date:** 2026-09-24
* **Scope:** Akasa Micro-Kernel Architecture, Extension SDK, & Worker Engine

## Context
As Akasa expanded to support complex multi-stage background workflows and dynamically mounted extensions, three architectural requirements emerged:
1. Extension routes lacked automated structural OpenAPI schema reflection for downstream client generation.
2. Background worker chaining relied on ad-hoc step execution without dynamic priority ordering, topological sorting, or centralized pipeline state reduction.
3. Unhandled exceptions in background workers, VFS commit loops, and hook handlers were logged to stdout without uniform OS-level error broadcasting.

## Decision
1. **OpenAPI 3.0 Schema Reflection:** Introduced `typeddict_to_openapi()` in `akasa/utils.py` to convert Python `TypedDict` type hints into OpenAPI 3.0 JSON Schema definitions. `AkasaExtension.route()` now accepts `request_schema`, `response_schema`, and `docstring` parameters, populating `bp.route_schemas` for introspection.
2. **DAG Orchestration & State Reduction:** Implemented `resolve_dag_chain()` in `akasa/workers/__init__.py` for cycle-safe topological sorting of pipeline steps. `_execute_immediate_job()` automatically reduces `state_deltas` across step transitions into `chain_state` and normalizes outputs to the `ChainStepArtifact` contract envelope.
3. **Centralized Error Dispatch:** Standardized exception handling across `HookRegistry`, `_vfs_commit_worker`, and worker execution loops to emit `system_error` events via `hooks.emit_background('system_error', ...)`.
4. **Boot Concurrency Hardening:** Protected worker schema initialization using `_SCHEMA_INIT_LOCK` and SQLite busy-retry loops to prevent database lock collisions across multi-workspace startups.

## Consequences
* REST endpoints provide machine-readable schemas for client generation.
* Complex workflows execute with deterministic step ordering, automatic state accumulation, and error tracing.
* Unhandled runtime exceptions are broadcast uniformly over the OS event bus.