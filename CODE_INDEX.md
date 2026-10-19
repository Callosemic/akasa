# Akasa Kernel Code Index

This index serves as the architectural map. It outlines the core directories and their operational purpose to maintain a clear mental model of the ecosystem, preventing cognitive overload and logic drift.

```text
akasa/
├── .gitkeep                        # Git preservation marker
├── CHANGELOG.md                    # [comment required]
├── CODE_INDEX.md                   # Architectural directory map and module overview
├── README.md                       # Core substrate documentation and usage guide
├── akasa/                          # Central kernel package root
│   ├── auth/                       # Authentication blueprint, token exchange, and Fernet encryption
│   │   └── __init__.py             # Fitness test suite entry point and runner
│   ├── db.py                       # Thread-local SQLite connection pool and WAL schema migrations
│   ├── events.py                   # Thread-safe SSE event bus and client queue manager
│   ├── extension.py                # Extension SDK context wrappers, SettingsManager, and JobManager
│   ├── hooks.py                    # Priority event bus (HookRegistry) and authorization engine
│   ├── process.py                  # Volume-sandboxed process manager and execution wrapper
│   ├── py.typed                    # PEP 561 marker for static typing compliance
│   ├── sync.py                     # Kernel state heartbeat and system delta calculator
│   ├── types.py                    # Tier 1 type definitions, protocols, and event contracts
│   ├── uri.py                      # Universal Resource Identifier parser and normalizer
│   ├── utils.py                    # Dynamic workspace physics, JSON config helpers, and macro tools
│   ├── vfs.py                      # Virtual File System, volume router, and async write queue
│   └── workers/                    # Metronome worker engine and job ledger
│       └── __init__.py             # Fitness test suite entry point and runner
├── docs/                           # Architectural documentation and standards
│   ├── 00_index.md                 # Master documentation index
│   ├── 01_engineering_standards.md # Core engineering standards and audit guardrails
│   └── adrs/                       # Architectural Decision Records (ADRs)
│       ├── 0001_domain_agnostic_ioc_and_process_sandboxing.md # ADR 0001 specification
│       ├── 0002_thread_safe_caching_sse_bus_and_vfs_barriers.md # ADR 0002 specification
│       ├── 0003_openapi_reflection_dag_orchestration_and_error_hooks.md # ADR 0003 specification
│       ├── 0004_vfs_bulk_mutation_batching_and_atomic_consolidation.md # ADR 0004 specification
│       ├── 0005_parallel_dag_orchestration_vfs_ram_caching_and_inter_workspace_routing.md # ADR 0005 specification
│       └── 0006_resource_governance_bounded_executors_and_secret_lifecycle.md # ADR 0006 specification
├── pyproject.toml                  # Build system specification and dependencies
└── tests/                          # Architectural fitness tests and AST validators
    ├── fitness/                    # Fitness rule package directory
    │   ├── __init__.py             # Fitness test suite entry point and runner
    │   ├── core.py                 # Shared fitness test state, path collection, and violation reporter
    │   └── rules_python.py         # AST visitor rules enforcing Tier 1 substrate purity and guardrails
    └── fitness_functions.py        # CLI executable entry point for fitness validation
```
