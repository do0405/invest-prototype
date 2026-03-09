import os
import subprocess
import sys


def test_import_main_is_fast_enough():
    env = os.environ.copy()
    env["PYTHONDONTWRITEBYTECODE"] = "1"

    proc = subprocess.run(
        [sys.executable, "-c", "import time; t=time.time(); import main; print(time.time()-t)"],
        capture_output=True,
        text=True,
        env=env,
        check=True,
    )

    elapsed = float((proc.stdout or "0").strip().splitlines()[-1])
    assert elapsed < 5.0
