# config.py - makes src/ available for importing into ./run_tests.py,
# ./run_experiments.py, and ./run_experiments2.py

import os
from pathlib import Path

proj_root = str(Path(__file__).parent.parent)

if proj_root not in os.sys.path:
    os.sys.path.append(proj_root)

# for ./run_tests.py, make src/ available in subprocesses w/o needing
# to import config in each test_*.py file
if "PYTHONPATH" in os.environ and proj_root not in os.environ["PYTHONPATH"]:
    os.environ["PYTHONPATH"] = proj_root + os.pathsep + os.environ["PYTHONPATH"]
else:
    os.environ["PYTHONPATH"] = proj_root

