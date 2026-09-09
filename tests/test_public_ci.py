"""Public validation remains runnable without organization infrastructure."""

import re
from pathlib import Path

import yaml


def _assert_job_actions(job_id, job, revision) -> None:
    """Require pinned actions and the approved validation package."""
    has_pinned_checkout = False
    for step in job["steps"]:
        assert not step.get("continue-on-error")
        action = step.get("uses")
        if not action:
            continue
        assert re.fullmatch(r"[^@]+@[0-9a-f]{40}", action)
        if not action.startswith("actions/checkout@"):
            continue
        assert step["with"]["persist-credentials"] is False
        if step["with"].get("repository") != "EndurantDevs/endurant-ci":
            continue
        assert step["with"]["ref"] == revision
        assert step["with"]["path"] == "ci"
        has_pinned_checkout = True
    assert has_pinned_checkout or job_id == "smoke"


def test_public_ci_is_hosted_with_bounded_permissions_and_runs_import_checks():
    workflows = Path(__file__).resolve().parents[1] / ".github/workflows"
    assert sorted(path.name for path in workflows.iterdir()) == ["ci.yml"]
    text = (workflows / "ci.yml").read_text(encoding="utf-8")
    workflow = yaml.safe_load(text)
    assert set(workflow.get("on", workflow.get(True))) == {"pull_request", "push"}
    assert workflow.get("on", workflow.get(True))["pull_request"] == {
        "types": ["opened", "synchronize", "reopened", "edited"]
    }
    assert workflow.get("on", workflow.get(True))["push"] == {"branches": ["main", "dev"]}
    assert workflow["permissions"] == {"contents": "read", "pull-requests": "read", "actions": "read"}
    assert set(workflow["jobs"]) == {
        "smoke", "validate", "publish", "dev-image-publication", "artifact-cleanup",
    }
    job = workflow["jobs"]["smoke"]
    assert job["runs-on"] == "ubuntu-latest"
    assert "container" not in job
    assert "services" not in job
    assert not job.get("continue-on-error")
    commands = "\n".join(step.get("run", "") for step in job["steps"])
    assert "scripts/ci/public_hygiene.py" in commands
    assert "uv sync --locked --no-default-groups --group test" in commands
    assert "uv run --locked --no-default-groups --group test --no-sync pytest -q" in commands
    assert "pip" not in commands
    assert "test_process_" in commands or "tests/process/" in commands
    setup_uv = next(step for step in job["steps"] if step.get("name") == "Install uv and Python")
    assert setup_uv == {
        "name": "Install uv and Python",
        "uses": "astral-sh/setup-uv@20cfd1bf945f4377ade1205e4dbc17946fc9a30d",
        "with": {"version": "0.12.12", "python-version": "3.14.7", "enable-cache": False},
    }
    assert all(token not in text for token in ("secrets.", "vars.", "ghcr.io", "workflow_dispatch", "self-hosted"))
    for step in job["steps"]:
        assert not step.get("continue-on-error")
        if action := step.get("uses"):
            assert re.fullmatch(r"[^@]+@[0-9a-f]{40}", action)
            if action.startswith("actions/checkout@"):
                assert step["with"]["persist-credentials"] is False


def test_shared_validation_is_pinned_and_metadata_edits_preserve_real_checks():
    workflow_path = Path(__file__).resolve().parents[1] / ".github/workflows/ci.yml"
    workflow = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))
    metadata_only = (
        "github.event_name == 'pull_request' && github.event.action == 'edited' "
        "&& !github.event.changes.title && !github.event.changes.base"
    )
    assert workflow["run-name"] == "${{ " + metadata_only + " && 'CI metadata update' || 'CI' }}"
    assert workflow["concurrency"] == {
        "group": (
            "${{ " + metadata_only
            + " && format('ci-metadata-{0}', github.run_id) || github.event_name == 'push' "
            + "&& format('ci-push-{0}', github.run_id) || format('ci-{0}', github.ref) }}"
        ),
        "cancel-in-progress": "${{ github.event_name == 'pull_request' && !(" + metadata_only + ") }}",
    }
    labels_by_job = {"smoke": "portable import checks", "validate": "Tests and build",
                     "publish": "Coverage results", "dev-image-publication": "DEV image publication",
                     "artifact-cleanup": "CI artifact cleanup"}
    revision = workflow["jobs"]["validate"]["env"]["CI_REVISION"]
    assert re.fullmatch(r"[0-9a-f]{40}", revision)
    assert set(revision) != {"0"}
    for job_id, job in workflow["jobs"].items():
        label = labels_by_job[job_id]
        if job_id == "smoke":
            assert job["name"] == label
            assert job["if"] == "${{ success() }}"
        else:
            assert job["name"] == "${{ " + metadata_only + f" && '{label} (metadata only)' || '{label}' " + "}}"
            assert job["if"] == "${{ !(" + metadata_only + ") && (success()) }}"
        assert "uses" not in job
        assert job["runs-on"] == "ubuntu-latest"
        assert not job.get("continue-on-error")
        if job_id == "dev-image-publication":
            assert job["permissions"] == {
                "contents": "read", "pull-requests": "read", "actions": "read", "packages": "write",
            }
        elif job_id == "artifact-cleanup":
            assert job["permissions"] == {"contents": "read", "actions": "write"}
        else:
            assert all(permission in {"read", "none"} for permission in job.get("permissions", {}).values())
        _assert_job_actions(job_id, job, revision)
        if job_id not in {"smoke", "artifact-cleanup"}:
            assert job["env"]["CI_REVISION"] == revision
    assert workflow["jobs"]["publish"]["needs"] == "validate"
    assert workflow["jobs"]["dev-image-publication"]["needs"] == ["smoke", "publish"]
    cleanup = workflow["jobs"]["artifact-cleanup"]
    assert cleanup["needs"] == ["dev-image-publication"]
    assert cleanup["timeout-minutes"] == 10
    assert cleanup["steps"] == [
        {"name": "Check out trusted cleanup helper",
         "uses": "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1",
         "with": {"repository": "EndurantDevs/endurant-ci", "ref": revision,
                  "path": "ci", "persist-credentials": False}},
        {"name": "Remove validated CI intermediates",
         "env": {"GH_TOKEN": "${{ github.token }}", "PYTHONDONTWRITEBYTECODE": "1"},
         "run": "python3 ci/scripts/artifact_cleanup.py"},
    ]
