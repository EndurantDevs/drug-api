from pathlib import Path

import yaml

WORKFLOW_PATH = Path(".github/workflows/deploy-dev.yml")


def test_dev_deploy_requires_successful_exact_main_ci() -> None:
    workflow = yaml.load(WORKFLOW_PATH.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
    trigger = workflow["on"]
    authorize = workflow["jobs"]["authorize"]
    authorization_script = authorize["steps"][0]["with"]["script"]
    queue = workflow["jobs"]["queue"]
    queue_step = queue["steps"][1]

    assert "push" not in trigger
    assert trigger["workflow_run"] == {
        "workflows": ["CI"],
        "branches": ["main"],
        "types": ["completed"],
    }
    assert workflow["permissions"]["actions"] == "read"
    assert "workflow_run.conclusion == 'success'" in authorize["if"]
    assert "run.path === expectedWorkflowPath" in authorization_script
    assert "deploySha !== mainSha" in authorization_script
    assert "listWorkflowRuns" in authorization_script
    assert queue["needs"] == "authorize"
    assert queue_step["env"]["DEPLOY_SHA"] == "${{ needs.authorize.outputs.deploy_sha }}"
    assert "current_main_sha" in queue_step["run"]
    assert '"${DEPLOY_SHA} main --detach"' in queue_step["run"]
    assert "${GITHUB_SHA} main --detach" not in queue_step["run"]
