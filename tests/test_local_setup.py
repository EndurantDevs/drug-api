"""The documented environment can load the CLI without local services."""

import os
import subprocess
import sys
import tomllib
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
    assert "server" in result.stdout
    assert "worker" in result.stdout


def test_uv_lock_is_the_only_binary_dependency_install_path():
    root = Path(__file__).resolve().parents[1]
    project = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
    lock = tomllib.loads((root / "uv.lock").read_text(encoding="utf-8"))
    uv = project["tool"]["uv"]
    assert project["project"]["requires-python"] == ">=3.14"
    assert uv == {"package": False, "no-build": True, "required-version": "==0.12.11"}
    assert not (root / "requirements.txt").exists()
    assert not (root / "requirements-dev.txt").exists()
    assert all(
        package.get("wheels") and all(wheel["hash"].startswith("sha256:") for wheel in package["wheels"])
        for package in lock["package"]
        if "registry" in package.get("source", {})
    )
    dockerfile = (root / "Dockerfile").read_text(encoding="utf-8")
    assert "ghcr.io/astral-sh/uv:0.12.11@sha256:" in dockerfile
    assert "uv sync --locked --no-dev" in dockerfile
    assert "pip install" not in dockerfile
