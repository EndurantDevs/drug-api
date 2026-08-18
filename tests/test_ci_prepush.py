"""Contracts for the repository-specific CI pre-push entry point."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_ci_uses_one_exact_prepush_gate() -> None:
    workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8"))
    runs = [
        step["run"]
        for job in workflow["jobs"].values()
        for step in job["steps"]
        if "run" in step
    ]

    assert runs == ["scripts/ci/prepush all"]
    assert workflow["env"]["PYTHON_VERSION"] == "3.13.15"


def test_prepush_keeps_monitor_provenance_and_coverage_headroom() -> None:
    prepush = (ROOT / "scripts/ci/prepush").read_text(encoding="utf-8")

    assert "HLTHPRT_SOURCE_COMMIT" in prepush
    assert "org.opencontainers.image.revision" in prepush
    assert "3.13.15" in prepush
    assert "require_frozen_candidate" in prepush
    assert "receipt.txt" in prepush
    assert "postgres:18@sha256:" in prepush
    assert "redis:7@sha256:" in prepush
    assert "import main, sanic" in prepush
    assert '--entrypoint /opt/venv/bin/python "$image_revision" /opt/scripts/monitor_openapi.py --check' in prepush
    assert "returntocorp/semgrep@sha256:" in prepush
    assert "--no-git-ignore" in prepush
    assert 'Redis.from_url(os.environ["HLTHPRT_REDIS_ADDRESS"]' in prepush
    assert 'for name in ("lines", "branches"):' in prepush
    assert 'if margin < 5:' in prepush
