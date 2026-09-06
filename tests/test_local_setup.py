"""The documented environment can load the CLI without local services."""

import os
import subprocess
import sys
from pathlib import Path

from dotenv import dotenv_values


def test_example_environment_loads_cli():
    root = Path(__file__).resolve().parents[1]
    environment_by_name = {key: value for key, value in os.environ.items() if not key.startswith("HLTHPRT_")}
    environment_by_name.update(dotenv_values(root / ".env.example"))
    result = subprocess.run(
        [sys.executable, "main.py", "--help"],
        cwd=root,
        env=environment_by_name,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "server" in result.stdout and "worker" in result.stdout
