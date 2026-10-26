import sys
import unittest
from pathlib import Path

AKASA_REPO_ROOT = Path(__file__).resolve().parent.parent
if AKASA_REPO_ROOT.as_posix() not in sys.path:
    sys.path.insert(0, AKASA_REPO_ROOT.as_posix())

if __name__ == "__main__":
    print("============================================================")
    print("              Akasa Kernel Unit Test Suite                  ")
    print("============================================================\n")

    loader = unittest.TestLoader()
    start_dir = (Path(__file__).resolve().parent / "unit").as_posix()
    suite = loader.discover(start_dir=start_dir, pattern="test_*.py")

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    print("\n============================================================")
    if result.wasSuccessful():
        print(f"✅ SUCCESS: All {result.testsRun} Akasa kernel unit test(s) passed.")
        sys.exit(0)
    else:
        failures = len(result.failures)
        errors = len(result.errors)
        print(f"❌ FAILED: {failures} failure(s), {errors} error(s) out of {result.testsRun} test(s).")
        sys.exit(1)