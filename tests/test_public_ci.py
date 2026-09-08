"""Public validation remains runnable without organization infrastructure."""

import re
from pathlib import Path

import yaml


def test_public_ci_is_hosted_read_only_and_runs_import_checks():
    workflows = Path(__file__).resolve().parents[1] / ".github/workflows"
    assert sorted(path.name for path in workflows.iterdir()) == ["ci.yml"]
    text = (workflows / "ci.yml").read_text(encoding="utf-8")
    workflow = yaml.safe_load(text)
    assert set(workflow.get("on", workflow.get(True))) == {"pull_request", "push"}
    assert workflow.get("on", workflow.get(True))["pull_request"] == {
        "types": ["opened", "synchronize", "reopened", "edited"]
    }
    assert workflow["permissions"] == {"contents": "read", "pull-requests": "read", "actions": "read"}
    assert set(workflow["jobs"]) == {"smoke", "validate", "publish"}
    job = workflow["jobs"]["smoke"]
    assert job["runs-on"] == "ubuntu-latest"
    assert "container" not in job and "services" not in job
    assert not job.get("continue-on-error")
    commands = "\n".join(step.get("run", "") for step in job["steps"])
    assert "scripts/ci/public_hygiene.py" in commands
    assert "python -m pytest -q" in commands
    assert "test_process_" in commands or "tests/process/" in commands
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
        "group": "${{ " + metadata_only + " && format('ci-metadata-{0}', github.run_id) || format('ci-{0}', github.ref) }}",
        "cancel-in-progress": "${{ !(" + metadata_only + ") && github.ref != 'refs/heads/main' }}",
    }
    labels = {"smoke": "portable import checks", "validate": "Tests and build", "publish": "Coverage results"}
    revision = workflow["jobs"]["validate"]["env"]["CI_REVISION"]
    assert re.fullmatch(r"[0-9a-f]{40}", revision) and set(revision) != {"0"}
    for job_id, job in workflow["jobs"].items():
        label = labels[job_id]
        assert job["name"] == "${{ " + metadata_only + f" && '{label} (metadata only)' || '{label}' " + "}}"
        assert job["if"] == "${{ !(" + metadata_only + ") && (success()) }}"
        assert "uses" not in job and job["runs-on"] == "ubuntu-latest"
        assert not job.get("continue-on-error")
        assert all(value in {"read", "none"} for value in job.get("permissions", {}).values())
        pinned_checkout = False
        for step in job["steps"]:
            assert not step.get("continue-on-error")
            if action := step.get("uses"):
                assert re.fullmatch(r"[^@]+@[0-9a-f]{40}", action)
                if action.startswith("actions/checkout@"):
                    assert step["with"]["persist-credentials"] is False
                    if step["with"].get("repository") == "EndurantDevs/endurant-ci":
                        assert step["with"]["ref"] == revision
                        assert step["with"]["path"] == "ci"
                        pinned_checkout = True
        assert pinned_checkout or job_id == "smoke"
        if job_id != "smoke":
            assert job["env"]["CI_REVISION"] == revision
    assert workflow["jobs"]["publish"]["needs"] == "validate"
