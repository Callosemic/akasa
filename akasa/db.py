from pathlib import Path
import os
import time
import shutil
import sqlite3
import threading
from akasa.utils import get_workspace_physics, get_all_workspace_ids, sniff_tenant_id
from akasa.hooks import hooks

# Thread-local storage guarantees safe connection pooling across the ASGI / Worker matrix
_local = threading.local()

_REGISTERED_SCHEMAS = {}

def register_schema(ext_name, schema_dict):
    """Registers a declarative SQLite schema for automatic workspace migration."""
    _REGISTERED_SCHEMAS[ext_name] = schema_dict

def apply_declarative_schema(db_name, schema_dict, workspace_id=None):
    """Generates CREATE TABLE and executes ALTER TABLE ADD COLUMN migrations via diffing."""
    conn = get_connection(db_name, workspace_id)
    for table_name, columns in schema_dict.items():
        # 1. Generate CREATE TABLE
        col_defs = ", ".join([f"{col} {dtype}" for col, dtype in columns.items()])
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({col_defs})")

        # 2. Diff columns and ALTER TABLE
        cursor = conn.execute(f"PRAGMA table_info({table_name})")
        existing_cols = {row['name'] for row in cursor.fetchall()}

        for col, dtype in columns.items():
            if col not in existing_cols:
                try:
                    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {col} {dtype}")
                except Exception as e:
                    print(f"⚠️ Auto-migration failed for {table_name}.{col}: {e}")
    conn.commit()
# Register core worker database schema for tracking all file deltas and worker ledgers contextually
register_schema('workers', {
    'vfs_event_log': {
        'filepath': 'TEXT PRIMARY KEY',
        'mutation_type': 'TEXT',
        'timestamp': 'REAL'
    },
    'jobs': {
        'id': 'TEXT PRIMARY KEY',
        'ext_name': 'TEXT',
        'callback_name': 'TEXT',
        'interval_ms': 'INTEGER',
        'jitter_ms': 'INTEGER DEFAULT 0',
        'next_run_at': 'REAL',
        'status': 'TEXT',
        'args_json': 'TEXT'
    },
    'ephemeral_artifacts': {
        'id': 'TEXT PRIMARY KEY',
        'filepath': 'TEXT',
        'module_owner': 'TEXT',
        'created_at': 'REAL',
        'expires_at': 'REAL'
    },
    'immediate_jobs': {
        'id': 'TEXT PRIMARY KEY',
        'ext_name': 'TEXT',
        'callback_name': 'TEXT',
        'status': 'TEXT',
        'status_message': 'TEXT',
        'artifact_json': 'TEXT',
        'meta_json': 'TEXT',
        'created_at': 'REAL',
        'updated_at': 'REAL',
        'args_json': 'TEXT',
        'job_category': "TEXT DEFAULT 'system_background'"
    }
})
@hooks.on('system_boot', priority=90)
def init_declarative_schemas(**kwargs):
    """Automatically provisions schemas and boots workspaces across all tenants."""
    for ws_id in get_all_workspace_ids():
        for ext_name, schema in _REGISTERED_SCHEMAS.items():
            apply_declarative_schema(ext_name, schema, ws_id)
        hooks.emit('workspace_boot', workspace_id=ws_id)
@hooks.on('workspace_shutdown')
def close_workspace_connections(workspace_id=None, **kwargs):
    """Evicts and closes thread-local SQLite connections for an unmounting tenant workspace."""
    if not workspace_id or not hasattr(_local, 'connections'):
        return
    ws_dict = _local.connections.pop(workspace_id, None)
    if isinstance(ws_dict, dict):
        for conn in ws_dict.values():
            try:
                conn.close()
            except Exception:
                pass

def evict_cached_connection(workspace_id, db_name):
    """Safely closes and removes a single cached database connection on the current thread."""
    if not hasattr(_local, 'connections'):
        return
    ws_dict = _local.connections.get(workspace_id)
    if isinstance(ws_dict, dict) and db_name in ws_dict:
        try:
            ws_dict[db_name].close()
        except Exception:
            pass
        del ws_dict[db_name]

def close_thread_connections():
    """Closes and purges all cached SQLite connections on the calling thread to prevent FD leaks in per-request WSGI threads."""
    if not hasattr(_local, 'connections'):
        return
    conns_map = _local.connections
    _local.connections = {}
    for ws_dict in conns_map.values():
        if isinstance(ws_dict, dict):
            for conn in ws_dict.values():
                try:
                    conn.close()
                except Exception:
                    pass

def get_connection(db_name, workspace_id=None):
    """
    Returns a thread-local SQLite connection.
    Groups connections by workspace_id -> db_name to support stateless multi-tenancy
    without prematurely evicting active databases within the same workspace.
    """
    if not hasattr(_local, 'connections'):
        _local.connections = {}
    if not workspace_id:
        workspace_id = sniff_tenant_id()

    # True Workspace-Level & DB-Level LRU: Move accessed workspace_id and db_name to the end
    if workspace_id in _local.connections:
        ws_conns = _local.connections.pop(workspace_id)
        _local.connections[workspace_id] = ws_conns
        if db_name in ws_conns:
            conn = ws_conns.pop(db_name)
            ws_conns[db_name] = conn
            return conn
        if len(ws_conns) >= 4:
            oldest_db = next(iter(ws_conns))
            old_c = ws_conns.pop(oldest_db, None)
            if old_c:
                try:
                    old_c.close()
                except Exception:
                    pass
    else:
        # Evict oldest workspace when more than 3 distinct workspaces are open on this thread
        if len(_local.connections) >= 3:
            oldest_ws = next(iter(_local.connections))
            old_conns = _local.connections.pop(oldest_ws, {})
            if isinstance(old_conns, dict):
                for old_c in old_conns.values():
                    try:
                        old_c.close()
                    except Exception:
                        pass
        _local.connections[workspace_id] = {}

    cfg_path, _ = get_workspace_physics(workspace_id)
    db_dir = Path(cfg_path).parent.joinpath("cache", "db")
    db_path = db_dir.joinpath(f"{db_name}.db").as_posix()
    os.makedirs(db_dir, exist_ok=True)

    def _connect():
        conn = sqlite3.connect(db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        conn.execute("PRAGMA cache_size=-2000;")
        conn.commit()
        return conn

    try:
        conn = _connect()
    except sqlite3.DatabaseError as e:
        if "malformed" in str(e).lower() or "corrupt" in str(e).lower():
            print(f"⚠️ [Akasa DB] Malformed database detected at '{db_path}'. Healing database...")
            try:
                sqlite3.connect(db_path).close()
            except Exception:
                pass
            corrupt_path = f"{db_path}.corrupt_{int(time.time())}"
            try:
                if os.path.exists(db_path):
                    shutil.move(db_path, corrupt_path)
                for ext in ["-wal", "-shm"]:
                    if os.path.exists(db_path + ext):
                        os.remove(db_path + ext)
            except Exception as move_err:
                print(f"⚠️ [Akasa DB] Failed to move corrupt db: {move_err}")

            conn = _connect()
            if db_name in _REGISTERED_SCHEMAS:
                apply_declarative_schema(db_name, _REGISTERED_SCHEMAS[db_name], workspace_id)
        else:
            raise
    _local.connections[workspace_id][db_name] = conn
    return conn