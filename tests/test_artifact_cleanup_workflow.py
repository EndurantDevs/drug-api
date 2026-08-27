from pathlib import Path

import yaml

WORKFLOW_PATH = Path(".github/workflows/artifact-cleanup.yml")
CI_WORKFLOW_PATH = Path(".github/workflows/ci.yml")


def test_completed_ci_artifacts_are_deleted() -> None:
    workflow = yaml.load(WORKFLOW_PATH.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)

    assert workflow["on"] == {
        "workflow_run": {
            "workflows": ["CI", "Trusted pull request CI"],
            "types": ["completed"],
        },
        "pull_request_target": {"types": ["closed"]},
        "schedule": [{"cron": "17 3 * * *"}],
        "workflow_dispatch": "",
    }
    assert workflow["permissions"] == {"actions": "write", "contents": "read"}
    assert workflow["concurrency"] == {
        "group": "actions-artifact-cleanup",
        "cancel-in-progress": "false",
    }

    job = workflow["jobs"]["delete-completed-artifacts"]
    assert job["if"] == (
        "github.event_name == 'workflow_run' && "
        "github.event.workflow_run.conclusion == 'success'"
    )
    assert job["runs-on"] == "ubuntu-latest"
    assert job["timeout-minutes"] == "5"
    delete = job["steps"][0]
    assert delete["env"] == {
        "GH_TOKEN": "${{ github.token }}",
        "RUN_ID": "${{ github.event.workflow_run.id }}",
    }
    assert "/actions/runs/${RUN_ID}/artifacts?per_page=100" in delete["run"]
    assert 'artifact_ids="$(mktemp)"' in delete["run"]
    assert 'trap \'rm -f "$artifact_ids"\' EXIT' in delete["run"]
    assert 'done < "$artifact_ids"' in delete["run"]
    assert "/actions/artifacts/${artifact_id}" in delete["run"]


def test_ci_artifacts_expire_after_one_day() -> None:
    ci = yaml.load(CI_WORKFLOW_PATH.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
    upload = next(
        step
        for step in ci["jobs"]["prepush"]["steps"]
        if "actions/upload-artifact@" in step.get("uses", "")
    )
    assert upload["with"]["retention-days"] == "1"


def test_pr_close_and_stale_cleanup_are_scoped() -> None:
    workflow = yaml.load(WORKFLOW_PATH.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
    closed = workflow["jobs"]["delete-closed-pr-artifacts"]
    assert closed["if"] == "github.event_name == 'pull_request_target'"
    assert ".workflow_run.head_branch == $head_ref" in closed["steps"][0]["run"]
    assert closed["steps"][0]["env"]["PR_NUMBER"] == (
        "${{ github.event.pull_request.number }}"
    )
    assert '.status == "completed"' in closed["steps"][0]["run"]
    assert "any(.pull_requests[]?;" in closed["steps"][0]["run"]
    assert 'artifact_rows="$(mktemp)"' in closed["steps"][0]["run"]
    assert 'trap \'rm -f "$artifact_rows"\' EXIT' in closed["steps"][0]["run"]
    assert 'done < "$artifact_rows"' in closed["steps"][0]["run"]

    stale = workflow["jobs"]["delete-stale-artifacts"]
    assert "github.event_name == 'schedule'" in stale["if"]
    assert "1 day ago" in stale["steps"][0]["run"]
    assert 'artifact_ids="$(mktemp)"' in stale["steps"][0]["run"]
    assert 'trap \'rm -f "$artifact_ids"\' EXIT' in stale["steps"][0]["run"]
    assert 'done < "$artifact_ids"' in stale["steps"][0]["run"]
    assert ".workflow_run.id" not in stale["steps"][0]["run"]
    assert "/actions/runs/${run_id}" not in stale["steps"][0]["run"]
