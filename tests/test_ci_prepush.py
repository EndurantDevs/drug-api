"""Contracts for the repository-specific CI pre-push entry point."""

import os
import re
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_ROOT = ROOT / ".github/workflows"
ARC_RUNNER = (
    "${{ (github.event_name == 'push' || github.event_name == 'workflow_dispatch') && "
    "github.ref == 'refs/heads/main' && vars.DRUG_API_CI_RUNNER || 'ubuntu-latest' }}"
)
SELF_HOSTED_RUNNER = "${{ vars.DRUG_API_CI_RUNNER }}"
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
    assert triggers["push"]["branches"] == ["main"]
    assert triggers["workflow_dispatch"]["inputs"]["base_sha"] == {
        "description": "Exact comparison base commit",
        "required": True,
        "type": "string",
    }
    assert workflow["jobs"]["prepush"]["runs-on"] == ARC_RUNNER
    assert workflow["permissions"] == {"contents": "read"}


def test_reusable_ci_guards_the_self_hosted_gate() -> None:
    caller = yaml.safe_load((WORKFLOW_ROOT / "ci.yml").read_text(encoding="utf-8"))
    reusable = yaml.safe_load(
        (WORKFLOW_ROOT / "ci-self-hosted.yml").read_text(encoding="utf-8")
    )
    triggers = reusable.get("on", reusable.get(True))
    authorize = reusable["jobs"]["authorize"]
    prepush = reusable["jobs"]["prepush"]

    assert triggers == {
        "workflow_call": {
            "inputs": {
                "base_sha": {
                    "description": "Exact comparison base commit",
                    "required": True,
                    "type": "string",
                }
            }
        }
    }
    assert reusable["permissions"] == {"contents": "read"}
    assert authorize["runs-on"] == "ubuntu-latest"
    assert prepush["needs"] == "authorize"
    assert prepush["runs-on"] == SELF_HOSTED_RUNNER
    assert prepush["steps"][0] == {
        "name": "Require self-hosted runner",
        "if": "${{ runner.environment != 'self-hosted' }}",
        "run": "exit 1",
    }
    assert prepush["steps"][1:] == caller["jobs"]["prepush"]["steps"]


def _caller_guard_exit_code(context_overrides_map: dict[str, str]) -> int:
    """Run the protected caller guard with one synthetic GitHub context."""
    reusable = yaml.safe_load(
        (WORKFLOW_ROOT / "ci-self-hosted.yml").read_text(encoding="utf-8")
    )
    guard_script = reusable["jobs"]["authorize"]["steps"][0]["run"]
    trusted_environment_map = {
        "REPOSITORY": "EndurantDevs/drug-api",
        "EVENT_NAME": "pull_request",
        "REF": "refs/pull/56/merge",
        "CALLER_WORKFLOW_REF": (
            "EndurantDevs/drug-api/.github/workflows/ci.yml@refs/pull/56/merge"
        ),
        "CALLED_WORKFLOW_REF": (
            "EndurantDevs/drug-api/.github/workflows/ci-self-hosted.yml@refs/heads/main"
        ),
        "PR_BASE_REPOSITORY": "EndurantDevs/drug-api",
        "PR_BASE_REF": "main",
        "PR_HEAD_REPOSITORY": "EndurantDevs/drug-api",
        "PR_HEAD_FORK": "false",
    }
    guard_process = subprocess.run(
        ["bash", "-c", guard_script],
        env={**os.environ, **trusted_environment_map, **context_overrides_map},
        check=False,
        capture_output=True,
        text=True,
    )
    return guard_process.returncode


@pytest.mark.parametrize(
    ("context_overrides_map", "is_accepted"),
    [
        ({}, True),
        ({"REPOSITORY": "outside/drug-api"}, False),
        ({"REF": "refs/pull/56/head"}, False),
        ({"PR_BASE_REPOSITORY": "outside/drug-api"}, False),
        ({"PR_BASE_REF": "release"}, False),
        ({"PR_HEAD_REPOSITORY": "outside/drug-api", "PR_HEAD_FORK": "true"}, False),
        (
            {
                "CALLER_WORKFLOW_REF": (
                    "EndurantDevs/drug-api/.github/workflows/other.yml@refs/pull/56/merge"
                )
            },
            False,
        ),
        (
            {
                "CALLED_WORKFLOW_REF": (
                    "EndurantDevs/drug-api/.github/workflows/"
                    "ci-self-hosted.yml@refs/heads/feature"
                )
            },
            False,
        ),
        ({"EVENT_NAME": "push", "REF": "refs/heads/main"}, False),
    ],
)
def test_reusable_ci_caller_guard_accepts_only_trusted_contexts(
    context_overrides_map: dict[str, str], is_accepted: bool
) -> None:
    assert (_caller_guard_exit_code(context_overrides_map) == 0) is is_accepted


def test_ci_actions_are_immutable_and_other_workflows_stay_hosted() -> None:
    paths = sorted((*WORKFLOW_ROOT.glob("*.yml"), *WORKFLOW_ROOT.glob("*.yaml")))
    for path in paths:
        workflow = yaml.safe_load(path.read_text(encoding="utf-8"))
        for job in workflow["jobs"].values():
            if path.name not in {"ci.yml", "ci-self-hosted.yml"}:
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

    assert re.search(
        r"if \[\[.*workflow_dispatch.*check_commit_messages\.py --range.*\n\s*elif",
        prepush,
        re.DOTALL,
    )
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
