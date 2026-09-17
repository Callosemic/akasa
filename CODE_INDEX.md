# akasa Code Index

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
│   ├── extension.py                # Extension SDK context wrappers, SettingsManager, and JobManager
│   ├── hooks.py                    # Priority event bus (HookRegistry) and authorization engine
│   ├── py.typed                    # PEP 561 marker for static typing compliance
│   ├── sync.py                     # Kernel state heartbeat and system delta calculator
│   ├── types.py                    # Tier 1 type definitions, protocols, and event contracts
│   ├── uri.py                      # Universal Resource Identifier parser and normalizer
│   ├── utils.py                    # Dynamic workspace physics, JSON config helpers, and macro tools
│   ├── vfs.py                      # Virtual File System, volume router, and async write queue
│   └── workers/                    # Metronome worker engine and job ledger
│       └── __init__.py             # Background task scheduler, step chaining, and job executor
├── docs/                           # [comment required]
│   ├── 00_index.md                 # [comment required]
│   └── 01_engineering_standards.md # [comment required]
└── pyproject.toml                  # Build system specification and dependencies
```
