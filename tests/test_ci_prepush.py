"""Contracts for the repository-specific CI pre-push entry point."""

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_ROOT = ROOT / ".github/workflows"
ARC_RUNNER = (
    "${{ (github.event_name == 'push' || github.event_name == 'workflow_dispatch') && "
    "github.ref == 'refs/heads/main' && vars.DRUG_API_CI_RUNNER || 'ubuntu-latest' }}"
)
PINNED_ACTION = re.compile(r"^[^./\s][^@\s]*@[0-9a-f]{40}$")


def test_ci_uses_one_exact_prepush_gate() -> None:
    workflow_text = (WORKFLOW_ROOT / "ci.yml").read_text(encoding="utf-8")
    workflow = yaml.safe_load(workflow_text)
    triggers = workflow.get("on", workflow.get(True))
    runs = [
        step["run"]
        for job in workflow["jobs"].values()
        for step in job["steps"]
        if "run" in step
    ]

    assert runs == ["scripts/ci/prepush all"]
    assert workflow["env"]["PYTHON_VERSION"] == "3.13.15"
    assert set(triggers) == {"pull_request", "push", "workflow_dispatch"}
    assert workflow["jobs"]["prepush"]["runs-on"] == ARC_RUNNER
    assert workflow["permissions"] == {"contents": "read"}


def test_ci_actions_are_immutable_and_other_workflows_stay_hosted() -> None:
    paths = sorted((*WORKFLOW_ROOT.glob("*.yml"), *WORKFLOW_ROOT.glob("*.yaml")))
    for path in paths:
        workflow = yaml.safe_load(path.read_text(encoding="utf-8"))
        for job in workflow["jobs"].values():
            if path.name != "ci.yml":
                assert job["runs-on"] == "ubuntu-latest", path.name
            for step in job["steps"]:
                action = step.get("uses")
                if action is None:
                    continue
                assert PINNED_ACTION.fullmatch(action), path.name
                if action.startswith("actions/checkout@"):
                    assert step.get("with", {}).get("persist-credentials") is False


def test_prepush_keeps_provenance_and_coverage_headroom() -> None:
    prepush = (ROOT / "scripts/ci/prepush").read_text(encoding="utf-8")

    assert "HLTHPRT_SOURCE_COMMIT" in prepush
    assert "org.opencontainers.image.revision" in prepush
    assert "3.13.15" in prepush
    assert "require_frozen_candidate" in prepush
    assert "receipt.txt" in prepush
    assert "postgres:18@sha256:" in prepush
    assert "redis:7@sha256:" in prepush
    assert "import main, sanic" in prepush
    assert "returntocorp/semgrep@sha256:" in prepush
    assert "--no-git-ignore" in prepush
    assert 'Redis.from_url(os.environ["HLTHPRT_REDIS_ADDRESS"]' in prepush
    assert 'for name in ("lines", "branches"):' in prepush
    assert 'if margin < 5:' in prepush
