from .rules_python import check_python_files

def run_all():
    """Runs every kernel fitness rule."""
    check_python_files()