import sys
import os
from pathlib import Path

# Add akasa to path so tests can run isolated
sys.path.insert(0, Path(__file__).resolve().parent.parent.parent.as_posix())

from fitness import run_all
from fitness.core import FitnessState

if __name__ == "__main__":
    print("============================================================")
    print("        Akasa Substrate Fitness Functions Validator         ")
    print("============================================================\n")

    run_all()

    print("============================================================")
    if FitnessState.violations_found == 0:
        print("✅ SUCCESS: Kernel complies with all Tier 1 substrate standards.")
        sys.exit(0)
    else:
        print(f"❌ FAILED: Found {FitnessState.violations_found} architectural violations.")
        sys.exit(1)