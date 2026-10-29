import os
from pathlib import Path

# --- CONFIGURATION ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
KERNEL_DIR = PROJECT_ROOT / "akasa"
# credentials.py writes its own files: it must work when the VFS pipeline cannot start (CREDENTIALS_STDLIB_ONLY).
VFS_WRITE_WHITELIST = ["vfs.py", "db.py", "state.py", "context.py", "utils.py", "__init__.py", "credentials.py"]
SQLITE_WHITELIST = ["db.py"]
FLASK_TRANSPORT_WHITELIST = ["__init__.py", "extension.py"]

KERNEL_RING_MAP = {
    "types": 0,
    "uri": 0,
    "context": 0,
    "events": 1,
    "hooks": 1,
    "db": 2,
    "vfs": 2,
    "state": 2,
    "utils": 2,
    "process": 3,
    "workers": 3,
    "sync": 3,
    "auth": 4,
    "extension": 4,
}

class FitnessState:
    violations_found = 0

def collect_unique_files(directories, extensions):
    seen = set()
    for directory in directories:
        for root, dirs, files in os.walk(directory):
            if "__pycache__" in dirs: dirs.remove("__pycache__")
            for file in files:
                if any(file.endswith(ext) for ext in extensions):
                    fp = (Path(root) / file).resolve()
                    if fp not in seen:
                        seen.add(fp)
                        yield fp

def report_violation(rule_name, filepath, line_num, message):
    FitnessState.violations_found += 1
    try:
        rel_path = Path(filepath).resolve().relative_to(PROJECT_ROOT.resolve())
        display_path = f"akasa/{rel_path.as_posix()}"
    except ValueError:
        display_path = Path(filepath).as_posix()

    print(f"❌ [VIOLATION: {rule_name}] {display_path}:{line_num}")
    print(f"   ↳ {message}\n")