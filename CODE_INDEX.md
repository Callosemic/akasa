# Akasa Kernel Code Index

This index serves as the architectural map. It outlines the core directories and their operational purpose to maintain a clear mental model of the ecosystem, preventing cognitive overload and logic drift.

```text
akasa/
├── .gitkeep                        # Git preservation marker
├── CHANGELOG.md                    # Auto-generated semantic release changelog
├── CODE_INDEX.md                   # Architectural directory map and module overview
├── README.md                       # Core substrate documentation and usage guide
├── akasa/                          # Central kernel package root
│   ├── __init__.py                 # Ring 4: Two-phase bootloader and WSGI transport adapter
│   │   ├── def tune_process_resources() # Tier 1 OS Resource Governance: Raises file descriptor limits, bounds glibc arenas, and caps thread stack size.
│   │   ├── class ForceHTTPSProxyFix # WSGI Middleware to force HTTPS scheme when behind reverse proxies or trusted tunnel suffixes.
│   │   ├── def configure(control_dir, trusted_namespaces, extension_namespaces, core_modules, ...) # [stale?] Phase A: Headless-safe process & HookRegistry physics configuration.
│   │   ├── def init_app(app, token_header, public_routes, public_prefixes, ...) # [stale?] Phase B: Wires Flask transport, authentication gate, DB connection teardown, and optional SSE stream.
│   │   ├── def boot(skip_if_reloader_parent) # Ignites the Akasa kernel lifecycle (DB migrations, VFS write pipeline, and Worker Metronome).
│   │   └── def reset_state(cwd, control_dir) # [stale?] First-class test harness reset: delegates to each Ring subsystem's reset() contract.
│   ├── auth/                       # Ring 4: Authentication blueprint, token exchange, and Fernet encryption
│   │   ├── __init__.py             # Authentication and Fernet key rotation endpoints
│   │   │   ├── def get_master_key() # Loads or generates the global Fernet master key.
│   │   │   ├── def ensure_master_key_on_disk() # Synchronously guarantees the Fernet master key exists on disk.
│   │   │   ├── def encrypt_secret(val) # Encrypts a plaintext string using the active Fernet master key.
│   │   │   ├── def decrypt_secret(val) # Decrypts a v1: Fernet token string with fallback key support.
│   │   │   ├── def rotate_master_key(new_key_str) # Validates a new Fernet key, re-encrypts all workspace secrets.json files, and updates master_key.txt.
│   │   │   ├── def get_security_blueprint() # Lazily instantiates and returns the core security AkasaExtension blueprint.
│   │   │   └── def bootstrap()     # [stale?] Unauthenticated token exchange gate supporting absolute dynamic fallback routing.
│   │   └── peer.py                 # Ring 4 peer classifier: fail-closed proxy detection, trusted client IP, Host and Origin validation
│   │       ├── def register_proxy_header() # DI Interface: Registers additional headers whose presence marks a request as proxied (case-insensitive).
│   │       ├── def get_proxy_headers() # Returns the lowercase forwarding header names that mark a request as proxied.
│   │       ├── def register_allowed_host() # DI Interface: Registers exact hostnames (no port) accepted by Host validation, e.g.
│   │       ├── def get_allowed_hosts() # Returns the registered exact-match hostnames.
│   │       ├── def reset()         # Resets Ring 0 context state for isolated test harnesses.
│   │       ├── def parse_ip(value) # Parses an IP literal, tolerating IPv6 brackets, an IPv4 ':port' suffix, and IPv4-mapped IPv6.
│   │       ├── def is_loopback_ip(value) # True for 127.0.0.0/8, ::1, and IPv4-mapped loopback addresses.
│   │       ├── def normalize_hostname(value) # Lowercases a Host header value and strips any port, IPv6 brackets, and a trailing root dot.
│   │       ├── def rightmost_forwarded_ip(forwarded_for) # Returns the rightmost valid IP in an X-Forwarded-For value, or '' when absent or malformed.
│   │       ├── class PeerInfo      # Immutable classification of a request's origin.
│   │       ├── def classify_peer(remote_addr, headers) # Classifies a request from its socket peer address and headers.
│   │       ├── def is_host_allowed(host, trusted_suffixes, allowed_hosts) # DNS-rebinding guard: accepts loopback hostnames, exact registered hosts, and hosts under a trusted suffix.
│   │       └── def is_loopback_origin(url_str) # True when an Origin/Referer URL points at a loopback hostname (port ignored).
│   ├── context.py                  # Ring 0: Pure stdlib execution context (ExecutionFrame), atomic file writes, and stateless utilities
│   │   ├── class ExecutionFrame    # Immutable-style slotted execution frame bound to contextvars across HTTP, IPC, Worker, and CLI calls.
│   │   ├── def execution_frame(workspace_id, transport, method, payload, ...) # Context manager binding an active ExecutionFrame for transport-agnostic tenant and payload resolution.
│   │   ├── def get_current_frame() # Returns the currently active ExecutionFrame on this context/thread, or None.
│   │   ├── def set_tenant_resolver(resolver_fn) # DI Interface: Registers a transport-agnostic callable returning the active tenant workspace_id.
│   │   ├── def resolve_active_tenant(default) # Resolves the active workspace_id from the active ExecutionFrame or injected tenant_resolver.
│   │   ├── def sniff_tenant_id()   # Returns the active tenant workspace_id (defaults to 'default').
│   │   ├── def get_control_dir_name() # SSOT for resolving the active host control directory name (defaults to '.akasa').
│   │   ├── def get_daemon_cwd()    # Returns the anchored working directory of the Akasa daemon.
│   │   ├── def set_daemon_cwd(new_cwd) # Updates the anchored working directory of the Akasa daemon.
│   │   ├── def register_trusted_host_suffix() # DI Interface: Registers trusted reverse-proxy/tunnel hostname suffixes (e.g., '.internal.net').
│   │   ├── def get_trusted_host_suffixes() # Returns the tuple of registered trusted host suffixes.
│   │   ├── def reclaim_process_memory() # Tier 1 Kernel Memory Governance: Forces Python garbage collection and releases free glibc arenas back to the OS.
│   │   ├── def is_port_listening(port, host) # Checks if a TCP port is actively accepting connections on the given host.
│   │   ├── def can_bind_port(port, host) # Checks if a TCP port is free to bind on the given host interface.
│   │   ├── def allocate_ephemeral_port(host) # Asks the OS kernel for an available ephemeral TCP port.
│   │   ├── def atomic_write_bytes(filepath, data, mode, lock) # Atomically writes bytes to filepath using a dot-prefixed sibling temporary file, os.fsync, permission preservation, and os.replace.
│   │   ├── def atomic_write_text(filepath, content, encoding, mode, ...) # Atomically writes UTF-8 text to filepath via atomic_write_bytes.
│   │   ├── def clone_json_value(val) # Fast recursive container cloner for JSON-compatible structures (dicts/lists) without copy.deepcopy overhead.
│   │   ├── def thread_safe_cache(ttl, key_maker) # A non-blocking, single-flight TTL cache with invalidation race protection.
│   │   ├── def typeddict_to_openapi(td_class) # Transforms a Python TypedDict into an OpenAPI 3.0 JSON Schema object natively.
│   │   ├── def generate_idempotency_hash(payload) # Generates a deterministic JSON string hash for a dictionary payload.
│   │   ├── def parse_uri(path_str) # SDK Helper: Parses any scheme:// or relative path into (volume, relative_path).
│   │   ├── def fold_vfs_events(events) # Mathematical state machine for VFS event stream folding.
│   │   ├── def slugify(text)       # Normalizes arbitrary text into a clean lowercase alphanumeric slug.
│   │   ├── def is_subpath(child_path, parent_path) # Returns True if child_path resolves inside or equal to parent_path without prefix collision.
│   │   ├── def build_tree_dict(file_paths) # Builds a nested dictionary representing a directory hierarchy from a list of file paths.
│   │   ├── def generate_ascii_tree(file_paths) # Renders an ASCII directory tree string from a list of file paths.
│   │   ├── def resolve_dag_wavefronts(items, id_key, parents_key, order_key) # Compiles a list of node dictionaries into level-order topological wavefronts (generations) using stdlib graphlib.TopologicalSorter.
│   │   ├── def extract_template_variables(text) # Scans text for {{ var }} or {{ var | default }} placeholders (including {{ macro_var }}), ignoring system multipliers and wordcount tags.
│   │   ├── def interpolate_template_variables(text, values, resolver) # Inside-out template variable interpolator for {{ var }}, {{ var | fallback_var | default }}, and nested {{ var | {{ fallback }} }} placehold...
│   │   └── def reset()             # Resets Ring 0 context state for isolated test harnesses.
│   ├── db.py                       # Ring 2: Thread-local SQLite connection pool and WAL schema migrations
│   │   ├── def set_db_physics_resolvers(physics_fn, all_workspaces_fn) # DI Interface: Ring 2 state module binds workspace physics resolvers so db.py has zero upward dependencies.
│   │   ├── def register_schema(ext_name, schema_dict) # Registers a declarative SQLite schema for automatic workspace migration.
│   │   ├── def apply_declarative_schema(db_name, schema_dict, workspace_id) # Generates CREATE TABLE and executes ALTER TABLE ADD COLUMN migrations via diffing.
│   │   ├── def provision_workspace_schemas(workspace_id) # Applies all registered declarative SQLite schemas for a specific tenant workspace.
│   │   ├── def recover_corrupt_databases(workspace_id) # Self-healing recovery routine: salvages rows from *.db.corrupt_* dumps inside the workspace's cache/db directory.
│   │   ├── def migrate_legacy_sqlite_paths(workspace_id, db_name, table, column, ...) # Updates legacy substring paths inside a specific workspace SQLite table column if the database exists.
│   │   ├── def init_declarative_schemas(**kwargs) # Automatically provisions schemas and boots workspaces across all tenants.
│   │   ├── def close_workspace_connections(workspace_id, **kwargs) # Evicts and closes thread-local SQLite connections for an unmounting tenant workspace.
│   │   ├── def evict_cached_connection(workspace_id, db_name) # Safely closes and removes a single cached database connection on the current thread.
│   │   ├── def close_thread_connections() # Closes and purges all cached SQLite connections on the calling thread to prevent FD leaks in per-request WSGI threads.
│   │   ├── def reset()             # Resets Ring 0 context state for isolated test harnesses.
│   │   └── def get_connection(db_name, workspace_id) # Returns a thread-local SQLite connection.
│   ├── events.py                   # Thread-safe SSE event bus and client queue manager
│   │   └── class EventBus          # [comment required]
│   ├── extension.py                # Ring 4: Extension SDK context wrappers, SettingsManager, and JobManager
│   │   ├── def extension_auth(ext_name) # HTTP route decorator verifying that ext_name is enabled in the target workspace.
│   │   ├── class SettingsUpdateResponse(TypedDict) # [comment required]
│   │   ├── class JobManager        # [comment required]
│   │   ├── def get_registered_capabilities() # Returns a shallow copy of the global transport-agnostic capability registry.
│   │   ├── def get_registered_settings_schemas(workspace_id) # Evaluates and returns all registered declarative settings schemas for the target workspace.
│   │   ├── class SettingsManager   # [comment required]
│   │   ├── class StoreManager      # [comment required]
│   │   ├── class DatabaseWrapper   # Lightweight ORM/CRUD wrapper for centralized SQLite access.
│   │   ├── class ExtensionContext  # Pre-scoped context object injected into all SDK routes.
│   │   └── class AkasaExtension    # Agnostic Blueprint wrapper that enforces tenant isolation, API routing, authorization, and SQLite schema migrations for kernel extensions.
│   ├── hooks.py                    # Priority event bus (HookRegistry) and authorization engine
│   │   ├── class HookPayload_MutateWorkspaceConfig(TypedDict) # [comment required]
│   │   ├── class HookPayload_VFSMutated(TypedDict) # [comment required]
│   │   ├── class HookPayload_WorkspaceScoped(TypedDict) # [comment required]
│   │   ├── class HookPayload_WorkspaceShutdown(TypedDict) # [comment required]
│   │   ├── class HookPayload_RegisterManifestSignatures(TypedDict) # [comment required]
│   │   ├── class HookPayload_ExecuteIdentityHandshake(TypedDict) # [comment required]
│   │   ├── class HookPayload_IsTrustedOrigin(TypedDict) # [comment required]
│   │   ├── class HookPayload_VFSMountMissing(TypedDict) # [comment required]
│   │   ├── class HookPayload_PreFileSave(TypedDict) # [comment required]
│   │   ├── class HookPayload_JobBatonPassed(TypedDict) # [comment required]
│   │   ├── class HookPayload_SystemError(TypedDict) # [comment required]
│   │   └── class HookRegistry      # The central Event Bus for the Akasa Micro-Kernel.
│   ├── process.py                  # Volume-sandboxed process manager and execution wrapper
│   │   ├── def execute_binary(cmd, cwd, env, check, ...) # Universal binary execution wrapper resolving system PATH, venv directories, and module CLI fallbacks across systemd, desktop, and isolated e...
│   │   └── class ProcessManager    # [comment required]
│   ├── py.typed                    # PEP 561 marker for static typing compliance
│   ├── state.py                    # Ring 2: Workspace physics, extension enablement, and causal parsed-object JSON configuration cache
│   │   ├── def heal_daemon_config_files(remove_legacy_system_json) # Kernel-level daemon config healer: Synchronizes any legacy system.json into workspaces.json and host.json.
│   │   ├── def get_system_index_path() # Returns the canonical path to the daemon's workspaces.json routing index.
│   │   ├── def get_daemon_host_config_path() # Returns the canonical path to the daemon's host.json settings file.
│   │   ├── def get_json_cache_stats() # Returns telemetry statistics for the internal JSON configuration cache.
│   │   ├── def get_kernel_memory_telemetry() # Combines VFS read/write cache statistics and JSON config cache statistics into a single telemetry dict.
│   │   ├── def load_json_config(filepath, default_fallback) # Reads and parses a JSON configuration file backed by the VFS Read-Your-Writes cache.
│   │   ├── def save_json_config(filepath, data, workspace_id) # Stages a JSON configuration write onto the VFS pipeline and updates the parsed object cache atomically.
│   │   ├── def get_workspace_physics(workspace_id) # Resolves the canonical (config_path, workspace_root) 2-tuple for a tenant workspace.
│   │   ├── def get_physics_for(workspace_id) # Alias for get_workspace_physics(workspace_id).
│   │   ├── def get_tenant_control_dir(workspace_id) # Returns the control directory path for the specified tenant workspace.
│   │   ├── def get_all_workspace_ids() # Returns a list of all registered tenant workspace IDs (always including 'default').
│   │   ├── def load_config(workspace_id) # Loads the active tenant's config.json and applies mutate_workspace_config hook listeners.
│   │   ├── def register_core_module(ext_name) # DI Interface: Host OS declares its foundational modules.
│   │   ├── def get_core_modules()  # Returns a copy of the registered foundational core module names.
│   │   ├── def is_core_module(ext_name) # Returns True if ext_name is registered as a foundational core module.
│   │   ├── def get_active_modules(workspace_id) # Returns a sorted list of all active modules (registered core modules + enabled workspace extensions).
│   │   ├── def is_extension_enabled(ext_name, workspace_id) # Returns True if ext_name is a core module or enabled in the active workspace's extensions list.
│   │   ├── def resolve_sandbox_path(filepath, workspace_id) # Tier 1 Safe Path Resolution: Prevents directory traversal breakouts.
│   │   ├── def resolve_system_artifact_path(filepath, workspace_id) # Safely resolves a system artifact path inside active or registered tenant control directories.
│   │   └── def reset()             # Purges JSON configuration caches and load_config TTL cache for isolated test harnesses.
│   ├── sync.py                     # Ring 3: Kernel state heartbeat and system delta calculator
│   │   └── def get_system_deltas(workspace_id, since_ts) # Calculates lightweight system deltas and collects domain-specific signatures.
│   ├── types.py                    # Tier 1 type definitions, protocols, and event contracts
│   │   ├── class FileMutationPayload(TypedDict) # Payload emitted by VFS mutation events.
│   │   ├── class JobProgressPayload(TypedDict) # Payload tracking background worker progress.
│   │   ├── class JobSchedulePayload(TypedDict) # Payload defining a scheduled background job.
│   │   ├── class JobChainStep(TypedDict) # Defines a single execution step within a background worker chain.
│   │   ├── class JobChainPayload(TypedDict) # Payload formalizing the background worker step-chaining contract (ADR 0037).
│   │   ├── class WorkspacePathsDict(TypedDict) # Spatial path mappings for an active tenant workspace.
│   │   └── class VFSStorageEngine(Protocol) # Structural interface contract for VFS operations.
│   ├── uri.py                      # Ring 0: Universal Resource Identifier parser and normalizer
│   │   └── class AkasaURI          # Zero-knowledge Universal Resource Identifier.
│   ├── utils.py                    # Ring 2: Unified compatibility facade re-exporting Ring 0 (context) and Ring 2 (state)
│   ├── vfs.py                      # Ring 2: Virtual File System, volume router, and async write queue
│   │   ├── def set_vfs_artifact_path_resolver(resolver_fn) # DI Interface: Ring 2 state module binds resolve_system_artifact_path so vfs.py has zero upward dependencies.
│   │   ├── def has_pending_vfs_write(resolved_path) # Returns True if resolved_path currently has an un-flushed write in _VFS_PENDING_WRITES.
│   │   ├── def read_cached_physical_text(resolved_path, bypass_read_cache) # Unified Ring 2 Read-Your-Writes primitive: Inspects _VFS_PENDING_WRITES first (returning (-seq, content)), then _VFS_READ_CACHE (verifying s...
│   │   ├── def reset()             # [stale?] Purges VFS read cache, pending write overlay, and ancestor ring buffer for isolated test harnesses.
│   │   ├── def compute_vfs_sha256(text) # [comment required]
│   │   ├── def record_vfs_ancestor(workspace_id, filepath_or_resolved, text, text_hash) # [comment required]
│   │   ├── def lookup_vfs_ancestor(workspace_id, filepath_or_resolved, base_hash, fallback_base_content) # [comment required]
│   │   ├── def three_way_merge_text(base_text, disk_text, incoming_text) # Deterministic 3-Way Text Merger (Base -> Disk vs.
│   │   ├── def get_vfs_cache_stats() # Returns public telemetry statistics for the VFS read cache and write queue.
│   │   ├── def evict_vfs_cache(resolved_path, preserve_ancestors) # [comment required]
│   │   ├── class DefaultVFSDriver  # [comment required]
│   │   ├── def set_mount_authorizer(authorizer_fn) # DI Interface: Host OS registers its peer-to-peer mount authorization policy.
│   │   ├── class InterWorkspaceVFSDriver # Zero-copy Inter-Workspace VFS Driver (ws://<source_workspace_id>/<domain>/<relative_path>).
│   │   ├── def register_driver(scheme, driver_instance, volume) # [comment required]
│   │   ├── def mount_volume(workspace_id, scheme, volume, physical_target) # [comment required]
│   │   ├── def resolve_physical_path(filepath, workspace_id, is_absolute_artifact) # Public SSOT gateway for resolving a canonical URI to its physical host path.
│   │   ├── def sync_vfs_barrier(workspace_id, timeout) # Halts the calling thread until all VFS writes queued prior to this call are physically flushed.
│   │   ├── def start_vfs_pipeline(**kwargs) # [comment required]
│   │   ├── def stop_vfs_pipeline(**kwargs) # [comment required]
│   │   ├── def execute_vfs_move(workspace_id, filepath, dest_path) # [comment required]
│   │   ├── def execute_vfs_delete(workspace_id, filepath) # [comment required]
│   │   ├── def execute_vfs_bulk(workspace_id, operations) # [comment required]
│   │   ├── def execute_vfs_save(workspace_id, filepath, content, data) # [comment required]
│   │   ├── def execute_vfs_save_physical(workspace_id, filepath, content, data, ...) # [comment required]
│   │   └── class VFSTransaction    # Provides dual-mode immediate queue dispatch and atomic buffered context-manager transactions for VFS mutations.
│   └── workers/                    # Metronome worker engine and job ledger
│       └── __init__.py             # Unit test package initialization
│           ├── class ChainStepSchema(TypedDict) # [comment required]
│           ├── class ChainStepArtifact(TypedDict) # [comment required]
│           ├── def resolve_dag_chain(ctx, hook_name) # Generic Akasa DAG Orchestrator.
│           ├── def validate_worker_kwargs(ext_name, callback_name, kwargs, allow_extra) # Synchronously validates worker keyword arguments against the registered function signature and type annotations, and verifies strict JSON se...
│           ├── def validate_dag_chain_kwargs(steps, kwargs) # Validates shared pipeline kwargs across all steps in a DAG chain.
│           ├── def start_dag_chain(steps, workspace_id, on_complete_hook, job_category, **kwargs) # Parallel-capable DAG Chain Coordinator.
│           ├── def run_parallel(fn, items, max_workers) # Executes `fn(item)` across `items` using the shared bounded Akasa I/O ThreadPoolExecutor and deterministically closes thread-local SQLite co...
│           ├── def wait_for_jobs(workspace_id, prefix, job_id, timeout) # Blocks the calling thread until all matching in-flight immediate jobs complete or timeout elapses.
│           ├── def register_callback(ext_name, callback_name, func, raw_func) # Extensions register their background functions here to be triggered by the Metronome.
│           ├── def submit_job(job_id, ext_name, callback_name, interval_ms, ...) # Schedules a recurring background task in the SQLite Ledger.
│           ├── def submit_one_shot_job(job_id, ext_name, callback_name, delay_ms, ...) # Schedules a one-time delayed task.
│           ├── def cancel_job(job_id, workspace_id) # Cancels and removes a scheduled background job from the Metronome ledger.
│           ├── def pause_job(job_id, workspace_id) # Freezes a scheduled job in the Metronome ledger without deleting its record.
│           ├── def resume_job(job_id, workspace_id, reset_timer) # Restores a paused job to pending status, optionally recalculating next_run_at.
│           ├── def submit_immediate_job(job_id, ext_name, callback_name, args_json, ...) # Drops a task directly into the active ThreadPoolExecutor and logs its lifecycle for UI polling.
│           ├── def update_immediate_job_meta(job_id, meta_dict, workspace_id) # Helper for workers to safely update discrete metrics without altering stream status.
│           ├── def update_immediate_job_status(job_id, status, message, artifact, ...) # Helper for workers to update their streaming status.
│           ├── def register_ephemeral_artifact(filepath, owner, ttl_seconds, workspace_id) # [comment required]
│           ├── def sweep_ephemeral_artifacts(workspace_id) # Sweeps and deletes expired temporary files and old immediate jobs.
│           ├── def get_active_pipeline_modules(workspace_id) # Fast, single-query helper returning active_modules, pending_modules, and is_pipeline_active without scanning vfs_event_log or firing manifes...
│           ├── def reset()         # [stale?] Resets Ring 0 context state for isolated test harnesses.
│           ├── def start_workers(**kwargs) # [comment required]
│           └── def stop_workers(**kwargs) # [comment required]
├── docs/                           # Architectural documentation and standards
│   ├── 00_index.md                 # Master documentation index
│   ├── 01_engineering_standards.md # Core engineering standards and audit guardrails
│   ├── 02_kernel_api_and_ioc_contracts.md # External SDK, DI, and IoC hook interface contracts
│   └── adrs/                       # Architectural Decision Records (ADRs)
│       ├── 0004_vfs_bulk_mutation_batching_and_atomic_consolidation.md # ADR 0004 specification
│       ├── 0006_resource_governance_bounded_executors_and_secret_lifecycle.md # ADR 0006 specification
│       ├── 0007_kernel_bootloader_vfs_3way_merge_and_substrate_purity.md # ADR 0007 kernel bootloader, 3-way VFS merge, and substrate purity specification
│       ├── 0008_microbatched_vfs_dual_pool_workers_and_ipc_gateway.md # ADR 0008 micro-batched VFS commits, dual-pool workers, signature validation, and IPC gateway specification
│       ├── 0009_5ring_acyclic_dag_execution_frame_and_causal_state_caching.md # ADR 0009 5-ring acyclic DAG, ExecutionFrame contextvars, and causal state caching specification
│       ├── 0010_fail_closed_peer_trust_dns_rebinding_gate_and_dag_wavefronts.md # ADR 0010: Fail-Closed Peer Classification, DNS-Rebinding Host Gate, Topological DAG Wavefronts, and Test Sandbox Quiescing
│       ├── archived/               # Baseline architectural decisions assimilated into kernel
│       │   ├── 0001_domain_agnostic_ioc_and_process_sandboxing.md # ADR 0001 specification
│       │   └── 0002_thread_safe_caching_sse_bus_and_vfs_barriers.md # ADR 0002 specification
│       └── superseded/             # Obsolete architectural decisions replaced by newer ADRs
│           ├── .gitkeep            # Git preservation marker
│           ├── 0003_openapi_reflection_dag_orchestration_and_error_hooks.md # ADR 0003 specification
│           └── 0005_parallel_dag_orchestration_vfs_ram_caching_and_inter_workspace_routing.md # ADR 0005 specification
├── pyproject.toml                  # Build system specification and dependencies
└── tests/                          # Architectural fitness tests and unit test suite
    ├── fitness/                    # Fitness rule package directory
    │   ├── __init__.py             # Unit test package initialization
    │   │   └── def run_all()       # [comment required]
    │   ├── core.py                 # Shared fitness test state, path collection, and violation reporter
    │   │   ├── class FitnessState  # [comment required]
    │   │   ├── def collect_unique_files(directories, extensions) # [comment required]
    │   │   └── def report_violation(rule_name, filepath, line_num, message) # [comment required]
    │   └── rules_python.py         # AST visitor rules enforcing Tier 1 substrate purity and guardrails
    │       ├── class KernelFitnessVisitor(ast.NodeVisitor) # [comment required]
    │       └── def check_python_files() # [comment required]
    ├── fitness_functions.py        # CLI executable entry point for fitness validation
    ├── run_unit_tests.py           # CLI runner for Akasa kernel behavioral unit tests
    └── unit/                       # Behavioral unit tests for the Akasa kernel
        ├── __init__.py             # Unit test package initialization
        ├── test_auth_peer_trust.py # Tests for peer classification, the localhost bootstrap route, the Host gate, and the forwarding-header rule
        │   ├── class TestPeerClassification(unittest.TestCase) # S1: classify_peer is the single source of truth for request origin and fails closed on any proxy signal.
        │   ├── class TestBootstrapAndHostGate(unittest.TestCase) # S1: /auth/bootstrap only grants localhost trust to direct loopback requests; the gate rejects foreign Hosts.
        │   └── class TestForwardedHeaderMembraneRule(unittest.TestCase) # S1: the FORWARDED_HEADER_MEMBRANE fitness rule keeps forwarding-header reads inside akasa/auth/peer.py.
        ├── test_kernel_hardening.py # Unit tests for RFC 0048 VFS micro-batching, idempotency, slotted AkasaURI, hook auth caching, and SQLite PRAGMAs
        │   └── class TestAkasaKernelHardening(unittest.TestCase) # Behavioral unit tests for RFC 0048 Akasa Micro-Kernel Hardening (Refactors A–F).
        ├── test_settings_is_set.py # Tests that SettingsManager.is_set distinguishes saved values from schema defaults in every scope
        │   └── class TestSettingsIsSet(unittest.TestCase) # SettingsManager.is_set distinguishes saved values from schema defaults across every scope.
        ├── test_vfs_reset_drain.py # Tests that reset_state drains queued VFS writes before switching the daemon cwd
        │   └── class TestVfsResetDrain(unittest.TestCase) # reset_state() drains queued VFS writes first, so they land under the cwd they were queued for.
        ├── test_worker_contracts.py # Unit tests for signature-driven worker validation, DAG chains, and envelope isolation
        │   └── class TestAkasaWorkerContracts(unittest.TestCase) # [comment required]
        └── test_workers_reset_quiesce.py # Tests that reset_state stops the metronome and job pools so no background thread outlives its sandbox
            └── class TestWorkersResetQuiesce(unittest.TestCase) # reset_state() stops the metronome and job pools so no background thread outlives its sandbox.
```
