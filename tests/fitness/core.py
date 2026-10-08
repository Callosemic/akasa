import os
from pathlib import Path

# --- CONFIGURATION ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
KERNEL_DIR = PROJECT_ROOT / "akasa"
VFS_WRITE_WHITELIST = ["vfs.py", "db.py", "__init__.py"]
SQLITE_WHITELIST = ["db.py"]

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