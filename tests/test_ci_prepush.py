"""Contracts for the repository-specific CI pre-push entry point."""

import os
import re
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_ROOT = ROOT / ".github/workflows"
TRUSTED_PR_CALLER = WORKFLOW_ROOT / "trusted-pr-ci.yml"
COMBINED_RUNNER = " ".join(
    """${{
      (
        (
          (github.event_name == 'push' || github.event_name == 'workflow_dispatch') &&
          github.ref == 'refs/heads/main'
        ) ||
        (
          inputs.use_self_hosted == true &&
          github.repository == 'EndurantDevs/drug-api' &&
          github.event_name == 'pull_request' &&
          github.ref == format('refs/pull/{0}/merge', github.event.number) &&
          github.event.pull_request.base.repo.full_name == github.repository &&
          github.event.pull_request.base.ref == 'main' &&
          github.event.pull_request.head.repo.full_name == github.repository &&
          github.event.pull_request.head.repo.fork == false &&
          github.event.pull_request.user.type == 'User' &&
          github.event.pull_request.user.login != 'dependabot[bot]' &&
          !endsWith(github.actor, '[bot]') &&
          !endsWith(github.triggering_actor, '[bot]')
        )
      ) &&
      vars.DRUG_API_CI_RUNNER ||
      'ubuntu-latest'
    }}""".split()
)
CALLER_USE_SELF_HOSTED = " ".join(
    """${{
      github.repository == 'EndurantDevs/drug-api' &&
      github.event_name == 'pull_request' &&
      github.ref == format('refs/pull/{0}/merge', github.event.number) &&
      github.event.pull_request.base.repo.full_name == github.repository &&
      github.event.pull_request.base.ref == 'main' &&
      github.event.pull_request.head.repo.full_name == github.repository &&
      github.event.pull_request.head.repo.fork == false &&
      github.event.pull_request.user.type == 'User' &&
      github.event.pull_request.user.login != 'dependabot[bot]' &&
      !endsWith(github.actor, '[bot]') &&
      !endsWith(github.triggering_actor, '[bot]')
    }}""".split()
)
PINNED_ACTION = re.compile(r"^[^./\s][^@\s]*@[0-9a-f]{40}$")


def test_ci_uses_one_exact_prepush_gate() -> None:
    workflow_text = (WORKFLOW_ROOT / "ci.yml").read_text(encoding="utf-8")
    workflow = yaml.safe_load(workflow_text)
    triggers = workflow.get("on", workflow.get(True))
    prepush = workflow["jobs"]["prepush"]
    gate_runs = [
        step["run"]
        for job in workflow["jobs"].values()
        for step in job["steps"]
        if step.get("run") == "scripts/ci/prepush all"
    ]

    assert set(workflow["jobs"]) == {"prepush"}
    assert prepush["name"] == "exact pre-push gate"
    assert "if" not in prepush
    assert gate_runs == ["scripts/ci/prepush all"]
    assert workflow["env"]["PYTHON_VERSION"] == "3.13.15"
    assert set(triggers) == {
        "push",
        "workflow_dispatch",
        "workflow_call",
    }
    assert triggers["push"]["branches"] == ["main"]
    assert triggers["workflow_dispatch"]["inputs"]["base_sha"] == {
        "description": "Exact comparison base commit",
        "required": True,
        "type": "string",
    }
    assert triggers["workflow_call"]["inputs"] == {
        "base_sha": {
            "description": "Exact comparison base commit",
            "required": True,
            "type": "string",
        },
        "use_self_hosted": {
            "description": "Run the protected same-repository pull request gate",
            "required": True,
            "type": "boolean",
        },
    }
    assert " ".join(prepush["runs-on"].split()) == COMBINED_RUNNER
    assert workflow["permissions"] == {"contents": "read"}


def test_trusted_pr_caller_uses_only_the_protected_reusable_gate() -> None:
    caller_text = TRUSTED_PR_CALLER.read_text(encoding="utf-8")
    caller = yaml.safe_load(caller_text)
    triggers = caller.get("on", caller.get(True))
    job = caller["jobs"]["ci"]

    assert triggers == {
        "pull_request": {"types": ["opened", "synchronize", "reopened"]}
    }
    assert caller["permissions"] == {"contents": "read"}
    assert set(caller["jobs"]) == {"ci"}
    assert set(job) == {"uses", "with"}
    assert job["uses"] == "EndurantDevs/drug-api/.github/workflows/ci.yml@main"
    assert job["with"]["base_sha"] == "${{ github.event.pull_request.base.sha }}"
    assert " ".join(job["with"]["use_self_hosted"].split()) == CALLER_USE_SELF_HOSTED
    assert "secrets" not in caller_text
    assert "vars." not in caller_text
    assert "github.workflow_ref" not in caller_text
    assert "author_association" not in caller_text


def test_reusable_ci_guards_the_self_hosted_gate() -> None:
    workflow = yaml.safe_load((WORKFLOW_ROOT / "ci.yml").read_text(encoding="utf-8"))
    prepush = workflow["jobs"]["prepush"]

    assert "needs" not in prepush
    guard = prepush["steps"][0]
    assert guard["name"] == "Authorize protected caller on runner"
    assert guard["if"] == "${{ inputs.use_self_hosted == true }}"
    assert guard["shell"] == "bash"
    assert guard["env"] == {
        "USE_SELF_HOSTED": "${{ inputs.use_self_hosted }}",
        "REPOSITORY": "${{ github.repository }}",
        "EVENT_NAME": "${{ github.event_name }}",
        "REF": "${{ github.ref }}",
        "PR_NUMBER": "${{ github.event.number }}",
        "CALLED_WORKFLOW_REF": "${{ job.workflow_ref }}",
        "ACTOR": "${{ github.actor }}",
        "TRIGGERING_ACTOR": "${{ github.triggering_actor }}",
        "PR_BASE_REPOSITORY": "${{ github.event.pull_request.base.repo.full_name }}",
        "PR_BASE_REF": "${{ github.event.pull_request.base.ref }}",
        "PR_HEAD_REPOSITORY": "${{ github.event.pull_request.head.repo.full_name }}",
        "PR_HEAD_FORK": "${{ github.event.pull_request.head.repo.fork }}",
        "PR_AUTHOR_LOGIN": "${{ github.event.pull_request.user.login }}",
        "PR_AUTHOR_TYPE": "${{ github.event.pull_request.user.type }}",
        "RUNNER_LABEL": "${{ vars.DRUG_API_CI_RUNNER }}",
        "RUNNER_ENVIRONMENT": "${{ runner.environment }}",
    }
    assert "github.workflow_ref" not in str(prepush)
    assert "author_association" not in str(prepush)


def _run_caller_guard(
    context_overrides_map: dict[str, str],
) -> subprocess.CompletedProcess[str]:
    """Run the protected caller guard with one synthetic GitHub context."""
    workflow = yaml.safe_load((WORKFLOW_ROOT / "ci.yml").read_text(encoding="utf-8"))
    guard_script = workflow["jobs"]["prepush"]["steps"][0]["run"]
    trusted_environment_map = {
        "USE_SELF_HOSTED": "true",
        "REPOSITORY": "EndurantDevs/drug-api",
        "EVENT_NAME": "pull_request",
        "REF": "refs/pull/56/merge",
        "PR_NUMBER": "56",
        "CALLED_WORKFLOW_REF": (
            "EndurantDevs/drug-api/.github/workflows/ci.yml@refs/heads/main"
        ),
        "ACTOR": "trusted-user",
        "TRIGGERING_ACTOR": "trusted-user",
        "PR_BASE_REPOSITORY": "EndurantDevs/drug-api",
        "PR_BASE_REF": "main",
        "PR_HEAD_REPOSITORY": "EndurantDevs/drug-api",
        "PR_HEAD_FORK": "false",
        "PR_AUTHOR_LOGIN": "trusted-user",
        "PR_AUTHOR_TYPE": "User",
        "RUNNER_LABEL": "drug-api-main-ci",
        "RUNNER_ENVIRONMENT": "self-hosted",
    }
    return subprocess.run(
        ["bash", "-c", guard_script],
        env={**os.environ, **trusted_environment_map, **context_overrides_map},
        check=False,
        capture_output=True,
        text=True,
    )


@pytest.mark.parametrize(
    ("context_overrides_map", "is_accepted"),
    [
        ({}, True),
        ({"USE_SELF_HOSTED": "false"}, False),
        ({"REPOSITORY": "outside/drug-api"}, False),
        ({"REF": "refs/pull/56/head"}, False),
        (
            {
                "PR_NUMBER": "not-numeric",
                "REF": "refs/pull/not-numeric/merge",
            },
            False,
        ),
        ({"PR_BASE_REPOSITORY": "outside/drug-api"}, False),
        ({"PR_BASE_REF": "release"}, False),
        ({"PR_HEAD_REPOSITORY": "outside/drug-api"}, False),
        ({"PR_HEAD_FORK": "true"}, False),
        ({"PR_AUTHOR_TYPE": "Bot"}, False),
        ({"PR_AUTHOR_LOGIN": "dependabot[bot]"}, False),
        ({"ACTOR": "dependabot[bot]"}, False),
        ({"TRIGGERING_ACTOR": "renovate[bot]"}, False),
        ({"RUNNER_ENVIRONMENT": "github-hosted"}, False),
        (
            {
                "PR_NUMBER": "57",
            },
            False,
        ),
        (
            {
                "CALLED_WORKFLOW_REF": (
                    "EndurantDevs/drug-api/.github/workflows/ci.yml@refs/heads/feature"
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
    assert (_run_caller_guard(context_overrides_map).returncode == 0) is is_accepted


def test_reusable_ci_caller_guard_reports_missing_runner_label() -> None:
    result = _run_caller_guard({"RUNNER_LABEL": ""})

    assert result.returncode == 1
    assert result.stdout == "::error::DRUG_API_CI_RUNNER is not configured\n"


def test_ci_actions_are_immutable_and_other_workflows_stay_hosted() -> None:
    paths = sorted((*WORKFLOW_ROOT.glob("*.yml"), *WORKFLOW_ROOT.glob("*.yaml")))
    for path in paths:
        workflow = yaml.safe_load(path.read_text(encoding="utf-8"))
        if path == TRUSTED_PR_CALLER:
            continue
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
