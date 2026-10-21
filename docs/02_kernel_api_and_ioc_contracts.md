# 02: Akasa Kernel API & Inversion of Control (IoC) Contracts

**Identity**: The Kernel Architect.  
**Mission**: To define the complete, unambiguous external interface contract between the **Akasa** Tier 1 micro-kernel substrate and any host application or extension ecosystem mounted on top of it.

---

## 1. Environment Variables & Boot Configuration

Host applications configure Akasa's spatial physics and runtime diagnostics through four environment variables prior to importing or booting the kernel:

| Environment Variable | Default | Purpose |
| :--- | :--- | :--- |
| `AKASA_CONTROL_DIR` | `".akasa"` | Name of the hidden control plane directory inside the workspace root and user home directory (`~/<control_dir>/master_key.txt`). |
| `AKASA_CONFIG` | *Unset* | Optional absolute path override for the active workspace `config.json`. |
| `AKASA_BOOT_TOKEN` | *Auto-generated* | 32-character hex runtime session token (`BOOT_TOKEN`). Anchored in `os.environ` to survive `os.execv()` and reloader restarts. |
| `AKASA_DEBUG` | `"0"` | Set to `"1"` to enable verbose stdout telemetry for VFS physical writes, idempotency drops, and `vfs_mutated` broadcasts. |

---

## 1.1 Process Governance & Bootloader Lifecycle (`akasa`)

Host applications can wire the full Tier 1 kernel lifecycle via the top-level `akasa` module:

* **`akasa.tune_process_resources()`**: Raises `RLIMIT_NOFILE` to 65,536, bounds glibc arenas (`MALLOC_ARENA_MAX=2`), and sets thread stack size to 1 MB.
* **`akasa.configure(...)`**: Phase A headless configuration setting control directory, trusted namespaces, extension namespaces, core modules, SSE passthrough events, and trusted tunnel suffixes.
* **`akasa.init_app(app: Flask, ...)`**: Phase B transport wiring. Attaches `ForceHTTPSProxyFix`, mounts authentication blueprints (`auth_bp`, `security_bp`), sets up `@app.teardown_appcontext` SQLite connection purging, injects anti-caching response headers, enforces token gates, and mounts optional SSE streaming routes.
* **`akasa.boot(skip_if_reloader_parent=False)`**: Fires `hooks.emit('system_boot')`, gracefully bypassing parent processes during Werkzeug debug reloads.
* **`akasa.reset_state(cwd=None, control_dir=None)`**: Closes pooled database handles, purges JSON/VFS/Ancestor RAM caches, and resets workspace state for isolated test harnesses.

---

## 2. Host Bootstrapping & Dependency Injection (DI)

Because Akasa adheres to the **Blind Substrate Mandate**, the host application injects its domain topology, authorization policies, and volume mounts during startup:

### 2.1 Namespace & Core Module Registration
* **`hooks.register_trusted_namespace(namespace_prefix: str)`**: Registers a Python module prefix (e.g., `'insetu.core.'`) that bypasses per-workspace extension enablement checks on the Event Bus.
* **`hooks.register_extension_namespace(namespace_prefix: str)`**: Registers the host's Tier 3 extension module prefix (e.g., `'insetu.extensions.'`).
* **`hooks.register_sse_passthrough(*event_names: str)`**: Registers high-priority Event Bus hook names (e.g., `'topology_resolved'`) that `hooks.emit()` should automatically serialize and broadcast over `sse_bus`.
* **`hooks.set_workspace_router(router_fn: Callable)`**: Injects a spatial routing function `router_fn(event_name: str, **kwargs) -> Iterable[str]` that maps physical paths or global events to owning `workspace_id`s.
* **`register_core_module(ext_name: str)` (`akasa.utils`)**: Declares a module identifier as a foundational core module so `is_extension_enabled(ext_name)` always returns `True`.

### 2.2 VFS Volume & Driver Registration (`akasa.vfs`)
* **`mount_volume(workspace_id: str, scheme: str, volume: str, physical_target: str)`**: Maps a logical `(workspace_id, scheme, volume)` tuple in `_MOUNT_TABLE` to a physical host directory.
* **`register_driver(scheme: str, driver_instance: Any, volume: Optional[str] = None)`**: Registers a custom VFS resolution driver (implementing `.resolve(uri_obj, workspace_id, physical_target) -> Optional[str]`) for an entire URI scheme (e.g., `'ctx'`) or a specific `(scheme, volume)` pair.
* **`set_mount_authorizer(authorizer_fn: Callable)`**: Injects the bilateral peer-to-peer mount policy evaluator `authorizer_fn(target_ws: str, subscriber_ws: str, domain: str, rel_path: str) -> bool` governing read-only `ws://<target_ws>/<domain>/<rel_path>` virtual mounts.

### 2.3 Flask Blueprints & WSGI Teardown
* **`auth_bp` (`akasa.auth`)**: Exposes `POST /auth/bootstrap` for localhost, IoC hook (`execute_identity_handshake`), and static token authentication handshakes.
* **`security_bp` (`akasa.auth`)**: Core `AkasaExtension` exposing `/api/<workspace_id>/security/settings` for Fernet `master_fernet_key` inspection and rotation.
* **`close_thread_connections()` (`akasa.db`)**: Must be registered on `@app.teardown_appcontext` by the host Flask application to deterministically close thread-local SQLite handles after WSGI request completion.

---

## 3. Inversion of Control (IoC) Event Bus Contracts (`akasa.hooks`)

### 3.1 Query Hooks (Emitted by Akasa; Host Returns Data)

| Hook Name | Kwargs Passed by Akasa | Expected Host Return Value |
| :--- | :--- | :--- |
| `execute_identity_handshake` | `request`, `client_ip: str`, `config: dict`, `workspace_id: str` | `{"status": "authenticated", "method": str, "user": Optional[str]}` or `None`. |
| `is_trusted_origin` | `url: str`, `hostname: str`, `workspace_id: str` | `bool` (`True` if the `Origin`/`Referer` hostname is permitted during localhost bootstrap). |
| `mutate_workspace_config` | `cfg: dict` (positional arg 0), `workspace_id: str` | Mutates `cfg` in-place prior to caching in `load_config()`. |
| `request_paths` | `workspace_id: str` | `Dict[str, str]` of domain directory paths merged into `ctx.paths`. |
| `vfs_mount_missing` | `workspace_id: str`, `scheme: str`, `volume: str` | Side-effect hook: host may call `mount_volume()` JIT before `_resolve_physical_path()` returns `None`. |
| `request_vfs_ignore_dirs` | `workspace_id: str` | `List[str]` of directory names (e.g., `['.git', 'node_modules']`) skipped during `ctx.vfs.walk()`. |
| `expand_selection` | `items: list`, `workspace_id: str`, `gather_only: bool` | `List[str]` of expanded canonical URIs (falls back to native VFS walk if `None`). |
| `find_path_candidates` | `query_path: str`, `workspace_id: str`, `**kwargs` | `List[Dict[str, Any]]` of disambiguated file candidates for `ctx.find_path_candidates()`. |
| `register_manifest_signatures` | `workspace_id: str`, `since_ts: float` | `Dict[str, Dict[str, Any]]` mapping domain keys (e.g., `'vfs'`, `'ctx'`) to signature maps in `get_system_deltas()`. |
| `<ext>_settings_updated` | `workspace_id: str` | Optional `{"requires_refresh": bool, "job_id": str}` returned to the client on `POST /api/<ws>/<ext>/settings`. |

### 3.2 Lifecycle & Telemetry Broadcasts

| Hook Name | Direction | Payload & Contract |
| :--- | :--- | :--- |
| `system_boot` | Host -> Kernel | Ignites declarative schema migrations (priority 90), VFS commit thread, and Metronome worker pools (priority 10). Non-core extensions are banned from subscribing. |
| `workspace_boot` | Kernel -> Host | Emitted per tenant (`workspace_id`) after declarative SQLite schemas are migrated (Phase 1 read-only discovery). |
| `workspace_shutdown` | Host -> Kernel | Evicts thread-local SQLite connections and worker initialization flags for `workspace_id`. |
| `system_shutdown` | Host -> Kernel | Drains `_VFS_WRITE_QUEUE` and gracefully shuts down Metronome and I/O `ThreadPoolExecutor`s. |
| `config_mutated` | Bidirectional | Invalidates `@thread_safe_cache` on `load_config(workspace_id)`. |
| `pre_file_save` | Kernel -> Host | `workspace_id`, `filepath`, `resolved_path`, `content`, `data`. Fired immediately prior to physical VFS disk mutation. |
| `vfs_mutated` | Kernel -> Host | `workspace_id`, `mutations: List[FileMutationPayload]` (`filepath`, `operation`, `ignore_ledger`, optional `relocated_to`/`relocated_from`). |
| `job_baton_passed` | Kernel -> Host | `workspace_id`, `root_job_id`, `previous_job_id`, `next_job_id`, `ext_name`. Emitted during sequential or DAG step transitions. |
| `system_error` | Kernel -> Host | `workspace_id`, `source`, `error_type`, `message`, `traceback`, `payload` (JSON string). Emitted asynchronously via `emit_background` when a hook, worker, or VFS task raises an unhandled exception. |

---

## 4. Extension SDK Contract (`akasa.extension`)

### 4.1 `AkasaExtension`
```python
ext_bp = AkasaExtension(
    name: str,
    module_name: str,
    title: Optional[str] = None,
    description: str = "",
    schema: Optional[Dict[str, Dict[str, str]]] = None,
    core: bool = False,
    settings_schema: Optional[Union[List[Dict[str, Any]], Callable[[str], List[Dict[str, Any]]]]] = None
)

 * Auto-Mounted Settings Routes:
   * GET /api/<workspace_id>/<name>/settings?repo=<optional_repo> -> Returns all settings with secrets masked (••••••••).
   * POST /api/<workspace_id>/<name>/settings?repo=<optional_repo> -> Updates settings across daemon, workspace, repo, and secure tiers, flushes the VFS barrier, and emits <name>_settings_updated.
   * GET /api/<workspace_id>/<name>/settings/schema -> Returns the evaluated settings_schema.
 * Decorators & Factories:
   * @ext_bp.route(rule, request_schema=None, response_schema=None, docstring=None, **options): Mounts /api/<workspace_id>/<name>/<rule>, records OpenAPI 3.0 schemas on ext_bp.bp.route_schemas, enforces extension_auth (unless core=True), and injects ctx: ExtensionContext as the first argument.
   * @ext_bp.worker(task_name): Registers a background worker callback in the Metronome ledger (<name>:<task_name>) and injects ctx: ExtensionContext bound to the active job_id and workspace_id.
   * @ext_bp.on(event_name, priority=50): Subscribes to OS lifecycle events, automatically injecting ctx: ExtensionContext if the handler's first parameter is named ctx.
   * ext_bp.get_context(workspace_id: str, job_id: Optional[str] = None) -> ExtensionContext: Hydrates a standalone ExtensionContext outside of an HTTP request.
4.2 ExtensionContext (ctx)
 * Identity & Configuration:
   * ctx.ext_name: str, ctx.workspace_id: str, ctx.req: flask.Request
   * ctx.config -> Dict[str, Any]: Cached tenant config.json (hydrated via mutate_workspace_config).
   * ctx.paths -> Dict[str, str]: Contains config_path, control_dir, workspace_root, data_dir, cache_dir, artifacts_base, plus any paths injected via request_paths.
   * ctx.get_repo_data_path(repo: str, subpath: str = "") -> str: Returns vfs://<repo>/<control_dir>-data/<ext_name>[/<subpath>].
   * ctx.get_active_modules() -> List[str]: Returns active core modules and workspace-enabled extensions.
   * ctx.get_shared_mounts(domain: Optional[str] = None) -> List[Dict[str, str]]: Returns active peer mounts [{"workspace_id": ..., "domain": ..., "uri": "ws://..."}].
   * ctx.iter_shared_mount_files(domain: Optional[str] = None, exts: Optional[List[str]] = None) -> Iterator[Tuple[str, str]]: Yields (source_workspace_id, ws_file_uri).
   * ctx.emit_sse(event_type: str, payload: Dict[str, Any]): Dispatches events to the global real-time SSE stream.
 * Database (ctx.db -> DatabaseWrapper):
   * Scoped to <tenant_control_dir>/cache/db/<ext_name>.db.
   * ctx.get_db(db_name: Optional[str] = None) -> DatabaseWrapper: Returns an SQLite wrapper for any registered workspace database.
   * Methods: .execute(sql, params), .executemany(sql, seq), .commit(), .get_all(table, order_by=None), .get_by_id(table, id_val, id_col="id"), .insert_or_replace(table, data_dict), .update(table, data_dict, where_col, where_val), .delete(table, where_col, where_val).
 * Virtual File System (ctx.vfs -> VFSTransaction):
   * All paths must be canonical scheme:// URIs unless data={"is_absolute_artifact": True} is passed.
   * Methods: .read(filepath, is_absolute_artifact=False) -> Optional[str], .save(filepath, content, data=None), .delete(filepath, data=None), .move(filepath, dest_path, data=None), .bulk(operations), .walk(directory_path, exts=None) -> Iterator[str].
   * OCC & Merging: .compute_sha256(text), .record_ancestor(filepath, text), .lookup_ancestor(filepath, base_hash), .three_way_merge(base_text, disk_text, incoming_text), .evict_cache(resolved_path).
   * Supports with ctx.vfs: context manager to batch multiple mutations into a single atomic execute_vfs_bulk commit.
   * ctx.sync_vfs_barrier(timeout=5.0): Blocks until all preceding VFS writes are physically flushed to disk.
 * Background Jobs (ctx.jobs -> JobManager):
   * .submit(task_name, coalesce=False, job_category="system_background", job_id=None, **kwargs) -> str (coalesce may be False, True, or "args").
   * .submit_one_shot(task_name, delay_ms, job_id=None, **kwargs) -> str
   * .schedule(task_name, interval_ms, jitter_ms=0, job_id=None, **kwargs) -> str
   * .submit_chain(steps, on_complete_hook=None, job_category="system_background", **kwargs) -> Optional[str]
   * .map_parallel(fn, items) -> list: Executes fn across items on the bounded 4-worker Akasa-IO pool with automatic SQLite connection cleanup.
   * .wait_for(prefix=None, job_id=None, timeout=10.0) -> bool
   * .is_in_flight(task_name=None, prefix=None, args_like=None, window_sec=None) -> bool
   * .find_active_pipeline_job(worker_names: List[str], force_full: bool = False, window_sec: float = 300.0) -> Optional[str]
   * .get_scheduled_args(job_id) -> Optional[Dict[str, Any]]
   * .get_status(job_id) -> Optional[Dict[str, Any]]
   * .get_recent_logs(limit=20) -> List[Dict[str, Any]]
   * .cancel(job_id), .pause(job_id) -> bool, .resume(job_id, reset_timer=True) -> bool
   * .update_progress(message, artifact=None), .update_meta(meta_dict)
   * .register_ephemeral(filepath, ttl_seconds=3600, owner=None), .get_ephemerals(owner=None) -> List[str], .clear_ephemerals(owner=None)
 * Settings & JSON Store:
   * ctx.settings (SettingsManager): .get(key, default=None, repo=None, mask_secrets=False), .set(key, value, repo=None), .get_all(repo=None, mask_secrets=False), .update(payload_dict, repo=None).
   * ctx.store (StoreManager): .get(filename, key, default=None), .set(filename, key, value) (persisted in <tenant_control_dir>/data/<ext_name>/<filename>).
 * Subprocess Execution (ctx.exec -> ProcessManager):
   * Resolves binaries via execute_binary across system PATH, active virtual environments, and python module CLI fallbacks.
   * .run(cmd, volume: Optional[str] = None, **kwargs) -> subprocess.CompletedProcess (defaults to capture_output=True, text=True, timeout=60).
   * .popen(cmd, volume: Optional[str] = None, **kwargs) -> subprocess.Popen.
 * Path, DAG & Event Helpers:
   * ctx.resolve_path(filepath) -> Optional[str]
   * ctx.find_path_candidates(query_path, **kwargs) -> List[Dict]
   * ctx.resolve_dag_chain(hook_name: str) -> List[Dict[str, Any]]
   * ctx.expand_selection(items, gather_only=False) -> List[str]
   * ctx.emit(event_name, *args, **kwargs) -> list
   * ctx.emit_first(event_name, *args, default=None, predicate=None, **kwargs) -> Any
   * ctx.emit_merge_dict(event_name, *args, **kwargs) -> Dict[str, Any]
   * ctx.emit_merge_list(event_name, *args, **kwargs) -> List[Any]
   * ctx.emit_background(event_name, *args, **kwargs)
5. URI, VFS Concurrency & Telemetry Primitives
 * AkasaURI(raw_path: str) (akasa.uri):
   * Attributes: .scheme, .volume, .path, .is_dir, .basename, .parent -> AkasaURI.
   * Methods: .join(child_path: str) -> AkasaURI, .relative_to(base_uri: AkasaURI) -> str, .match(pattern: str) -> bool.
 * VFS OCC & 3-Way Merge (akasa.vfs):
   * compute_vfs_sha256(text: str) -> str
   * record_vfs_ancestor(workspace_id: str, filepath_or_resolved: str, text: str, text_hash: Optional[str] = None) -> str
   * lookup_vfs_ancestor(workspace_id: str, filepath_or_resolved: str, base_hash: Optional[str], fallback_base_content: Optional[str] = None) -> Optional[str]
   * three_way_merge_text(base_text: str, disk_text: str, incoming_text: str) -> Tuple[bool, str]
   * get_vfs_cache_stats() -> dict
 * System Heartbeat & SSE Bus (akasa.sync, akasa.events):
   * get_system_deltas(workspace_id=None, since_ts=0.0) -> dict
   * get_active_pipeline_modules(workspace_id=None) -> dict
   * sse_bus.subscribe() -> queue.Queue, sse_bus.unsubscribe(q), sse_bus.emit_sse(event_type: str, payload: dict)