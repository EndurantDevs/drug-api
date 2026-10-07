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
    assert sorted(path.name for path in workflows.iterdir()) == ["artifact-cleanup.yml", "ci.yml"]
    text = (workflows / "ci.yml").read_text(encoding="utf-8")
    workflow = yaml.safe_load(text)
    assert set(workflow.get("on", workflow.get(True))) == {"pull_request", "push"}
    assert workflow.get("on", workflow.get(True))["pull_request"] == {
        "types": ["opened", "synchronize", "reopened", "edited"]
    }
    assert workflow.get("on", workflow.get(True))["push"] == {"branches": ["main", "dev"]}
    assert workflow["permissions"] == {"contents": "read", "pull-requests": "read", "actions": "read"}
    assert set(workflow["jobs"]) == {
        "smoke",
        "validate",
        "publish",
        "dev-image-publication",
        "artifact-cleanup",
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
    assert "test_process_" in commands or "tests/process/" in commands
    managed_python = next(step for step in job["steps"] if step.get("name") == "Install uv-managed Python")
    assert set(managed_python) == {"name", "run"}
    assert "uv --no-config venv --managed-python --python 3.14.7" in managed_python["run"]
    assert '"$RUNNER_TEMP/uv-python.XXXXXX"' in managed_python["run"]
    assert '"$GITHUB_PATH"' in managed_python["run"]
    assert '"$GITHUB_ENV"' in managed_python["run"]
    assert "actions/setup-python@" not in text
    bootstrap = next(step for step in job["steps"] if step.get("name") == "Install pinned uv")
    assert set(bootstrap) == {"name", "run"}
    assert (
        "https://github.com/astral-sh/uv/releases/download/0.12.17/uv-x86_64-unknown-linux-gnu.tar.gz"
        in bootstrap["run"]
    )
    assert "fa82fd8dde8e8eefdecada6aa0889666556cfceb690d06e0c3bca49eb3070a63" in bootstrap["run"]
    assert "sha256sum --check --strict" in bootstrap["run"]
    assert "uv-x86_64-unknown-linux-gnu/uv uv-x86_64-unknown-linux-gnu/uvx" in bootstrap["run"]
    assert '"$GITHUB_PATH"' in bootstrap["run"]

    assert "pip install" not in "\n".join(step.get("run", "") for step in job["steps"])
    assert all(token not in text for token in ("secrets.", "vars.", "ghcr.io", "workflow_dispatch", "self-hosted"))
    for step in job["steps"]:
        assert not step.get("continue-on-error")
        if action := step.get("uses"):
            assert re.fullmatch(r"[^@]+@[0-9a-f]{40}", action)
            if action.startswith("actions/checkout@"):
                assert step["with"]["persist-credentials"] is False


def test_stale_artifact_cleanup_is_main_only_and_pinned():
    path = Path(__file__).resolve().parents[1] / ".github/workflows/artifact-cleanup.yml"
    workflow = yaml.safe_load(path.read_text(encoding="utf-8"))
    revision = yaml.safe_load((path.parent / "ci.yml").read_text())["jobs"]["validate"]["env"]["CI_REVISION"]
    assert workflow.get("on", workflow.get(True)) == {
        "schedule": [{"cron": "31 2 * * *"}],
        "workflow_dispatch": None,
    }
    assert workflow["permissions"] == {
        "contents": "read",
        "pull-requests": "read",
        "actions": "write",
    }
    assert workflow["concurrency"] == {
        "group": "public-artifact-cleanup",
        "cancel-in-progress": False,
    }
    assert set(workflow["jobs"]) == {"stale-cleanup"}
    job = workflow["jobs"]["stale-cleanup"]
    assert job["if"] == ("github.repository == 'EndurantDevs/drug-api' && github.ref == 'refs/heads/main'")
    assert job["runs-on"] == "ubuntu-latest"
    assert job["timeout-minutes"] == 15
    assert job["steps"] == [
        {
            "name": "Check out trusted artifact lifecycle",
            "uses": "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1",
            "with": {
                "repository": "EndurantDevs/endurant-ci",
                "ref": revision,
                "path": "ci",
                "persist-credentials": False,
            },
        },
        {"name": "Install pinned uv", "run": "bash ci/scripts/install_uv"},
        {"name": "Install uv-managed Python", "run": "bash ci/scripts/setup_python"},
        {
            "name": "Delete only obsolete authenticated artifacts",
            "env": {"GH_TOKEN": "${{ github.token }}", "PYTHONDONTWRITEBYTECODE": "1"},
            "run": "python3 ci/scripts/artifact_cleanup.py --stale",
        },
    ]


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
            "${{ "
            + metadata_only
            + " && format('ci-metadata-{0}', github.event.pull_request.number) || github.event_name == 'push' "
            + "&& format('ci-push-{0}', github.run_id) || format('ci-{0}', github.ref) }}"
        ),
        "cancel-in-progress": "${{ github.event_name == 'pull_request' }}",
    }
    labels_by_job = {
        "smoke": "portable import checks",
        "validate": "Tests and build",
        "publish": "Coverage results",
        "dev-image-publication": "DEV image publication",
        "artifact-cleanup": "CI artifact cleanup",
    }
    revision = workflow["jobs"]["validate"]["env"]["CI_REVISION"]
    assert re.fullmatch(r"[0-9a-f]{40}", revision)
    assert set(revision) != {"0"}
    for job_id, job in workflow["jobs"].items():
        label = labels_by_job[job_id]
        if job_id in {"smoke", "publish"}:
            assert job["name"] == label
            assert job["if"] == ("${{ always() }}" if job_id == "publish" else "${{ success() }}")
        else:
            assert job["name"] == "${{ " + metadata_only + f" && '{label} (metadata only)' || '{label}' " + "}}"
            assert job["if"] == "${{ !(" + metadata_only + ") && (success()) }}"
        assert "uses" not in job
        assert job["runs-on"] == "ubuntu-latest"
        assert not job.get("continue-on-error")
        if job_id == "dev-image-publication":
            assert job["permissions"] == {
                "contents": "read",
                "pull-requests": "read",
                "actions": "read",
                "packages": "write",
            }
        elif job_id == "artifact-cleanup":
            assert job["permissions"] == {"contents": "read", "actions": "write"}
        else:
            assert all(permission in {"read", "none"} for permission in job.get("permissions", {}).values())
        _assert_job_actions(job_id, job, revision)
        if job_id not in {"smoke", "artifact-cleanup"}:
            assert job["env"]["CI_REVISION"] == revision
    assert workflow["jobs"]["publish"]["needs"] == "validate"
    assert workflow["jobs"]["dev-image-publication"]["needs"] == ["smoke", "publish", "validate"]


def test_metadata_coverage_requires_full_validation():
    """Keep metadata coverage bound to full validation within the existing timeout."""
    path = Path(__file__).resolve().parents[1] / ".github/workflows/ci.yml"
    workflow = yaml.safe_load(path.read_text(encoding="utf-8"))
    metadata_only = (
        "github.event_name == 'pull_request' && github.event.action == 'edited' "
        "&& !github.event.changes.title && !github.event.changes.base"
    )
    coverage = workflow["jobs"]["publish"]
    assert coverage["timeout-minutes"] == 10
    gate = coverage["steps"][0]
    assert gate["name"] == "Require complete public validation"
    assert gate["env"]["METADATA_ONLY"] == "${{ " + metadata_only + " }}"
    assert gate["env"]["RESULTS"] == "${{ toJSON(needs.*.result) }}"
    assert "SECONDS + 540" in gate["run"]
    assert "--paginate --slurp" in gate["run"]
    assert "sort_by([(.run_started_at // .created_at), .id, .run_attempt])" in gate["run"]
    assert all(step["if"] == "${{ !(" + metadata_only + ") && (success()) }}" for step in coverage["steps"][1:])


def test_artifacts_expire_after_one_day_and_keep_exact_producer_bindings():
    path = Path(__file__).resolve().parents[1] / ".github/workflows/ci.yml"
    jobs = yaml.safe_load(path.read_text())["jobs"]
    revision = jobs["validate"]["env"]["CI_REVISION"]
    cleanup = jobs["artifact-cleanup"]
    assert cleanup["needs"] == ["dev-image-publication", "validate", "publish"]
    assert cleanup["timeout-minutes"] == 10
    assert cleanup["steps"] == [
        {
            "name": "Check out trusted cleanup helper",
            "uses": "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1",
            "with": {
                "repository": "EndurantDevs/endurant-ci",
                "ref": revision,
                "path": "ci",
                "persist-credentials": False,
            },
        },
        {"name": "Install pinned uv", "run": "bash ci/scripts/install_uv"},
        {"name": "Install uv-managed Python", "run": "bash ci/scripts/setup_python"},
        {
            "name": "Remove validated CI intermediates",
            "env": {
                "GH_TOKEN": "${{ github.token }}",
                "PYTHONDONTWRITEBYTECODE": "1",
                "IMAGE_ARTIFACT_ID": "${{ needs.validate.outputs.image_artifact_id }}",
                "MEASUREMENT_ARTIFACT_ID": "${{ needs.publish.outputs.measurement_artifact_id }}",
                "IMAGE_RECEIPT_ARTIFACT_ID": "${{ needs.dev-image-publication.outputs.receipt_artifact_id }}",
            },
            "run": "python3 ci/scripts/artifact_cleanup.py",
        },
    ]
    for job in jobs.values():
        for step in job["steps"]:
            if step.get("uses", "").startswith("actions/upload-artifact@"):
                assert step["with"]["retention-days"] == 1
                assert step["with"]["if-no-files-found"] == "error"
    for job_id, output, step_id in (
        ("validate", "image_artifact_id", "image-artifact"),
        ("publish", "measurement_artifact_id", "measurement-artifact"),
        ("dev-image-publication", "receipt_artifact_id", "receipt-artifact"),
    ):
        assert jobs[job_id]["outputs"][output] == "${{ steps." + step_id + ".outputs.artifact-id }}"
        step = next(step for step in jobs[job_id]["steps"] if step.get("id") == step_id)
        assert step["uses"].startswith("actions/upload-artifact@")
    publisher = jobs["dev-image-publication"]
    assert publisher["env"] == {
        "CI_REVISION": revision,
        "PYTHONDONTWRITEBYTECODE": "1",
        "IMAGE_ARTIFACT_ID": "${{ needs.validate.outputs.image_artifact_id }}",
        "MEASUREMENT_ARTIFACT_ID": "${{ needs.publish.outputs.measurement_artifact_id }}",
    }
    prepare = next(step for step in publisher["steps"] if step.get("id") == "image")
    assert prepare["env"] == {"GH_TOKEN": "${{ github.token }}"}
