# Akasa Kernel Code Index

This index serves as the architectural map. It outlines the core directories and their operational purpose to maintain a clear mental model of the ecosystem, preventing cognitive overload and logic drift.

```text
akasa/
├── .gitkeep                        # Git preservation marker
├── CODE_INDEX.md                   # Architectural directory map and module overview
├── README.md                       # Core substrate documentation and usage guide
├── akasa/                          # Central kernel package root
│   ├── auth/                       # Authentication blueprint, token exchange, and Fernet encryption
│   │   └── __init__.py             # Background task scheduler, step chaining, and job executor
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
│       └── __init__.py             # Background task scheduler, step chaining, and job executor
├── docs/                           # Architectural documentation and standards
│   ├── 00_index.md                 # Master documentation index
│   ├── 01_engineering_standards.md # Core engineering standards and audit guardrails
│   └── adrs/                       # Architectural Decision Records (ADRs)
│       ├── 0001_domain_agnostic_ioc_and_process_sandboxing.md # ADR 0001 specification
│       └── 0002_thread_safe_caching_sse_bus_and_vfs_barriers.md # [comment required]
├── pyproject.toml                  # Build system specification and dependencies
└── tests/                          # [comment required]
    ├── fitness/                    # [comment required]
    │   ├── __init__.py             # Background task scheduler, step chaining, and job executor
    │   ├── core.py                 # [comment required]
    │   └── rules_python.py         # [comment required]
    └── fitness_functions.py        # [comment required]
```
