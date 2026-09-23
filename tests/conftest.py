import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from src.common.config import load_config

_PACKAGES = load_config()["spark"]["config"]["spark.jars.packages"]
os.environ["PYSPARK_SUBMIT_ARGS"] = f"--packages {_PACKAGES} pyspark-shell"
