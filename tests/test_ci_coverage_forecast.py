"""Workflow contracts for the single-report coverage forecast."""

from __future__ import annotations

from pathlib import Path

import yaml

WORKFLOW_PATH = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "ci.yml"
PREPUSH_PATH = WORKFLOW_PATH.parents[2] / "scripts" / "ci" / "prepush"


def _workflow() -> dict:
    return yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8"))


def _steps_by_name(job_by_name: dict) -> dict[str, dict]:
    return {
        step_by_name["name"]: step_by_name
        for step_by_name in job_by_name["steps"]
        if "name" in step_by_name
    }


def test_ci_forecast_requires_exact_source_and_target_provenance():
    workflow_by_name = _workflow()
    test_job_by_name = workflow_by_name["jobs"]["test"]
    steps_by_name = _steps_by_name(test_job_by_name)

    checkout_step = test_job_by_name["steps"][0]
    forecast_step = steps_by_name["Test coverage forecast"]
    evidence_upload_step = steps_by_name["Upload coverage evidence"]
    forecast_upload_step = steps_by_name["Upload coverage forecast"]
    prepush = PREPUSH_PATH.read_text(encoding="utf-8")

    assert checkout_step["with"]["fetch-depth"] == 0
    assert checkout_step["with"]["ref"] == "${{ github.sha }}"
    assert "github.event.pull_request.base.sha || github.event.before" in str(
        forecast_step["env"]
    )
    assert forecast_step["run"].strip() == "scripts/ci/prepush coverage"
    assert "write-provenance" in prepush
    assert "forecast" in prepush
    assert 'PYTHON_LINE_MARGIN=5' in prepush
    assert 'PYTHON_BRANCH_MARGIN=5' in prepush
    assert "HEAD^" not in prepush
    assert forecast_step["if"] == "always()"
    assert evidence_upload_step["if"] == "always()"
    assert forecast_upload_step["if"] == "always()"


def test_ci_forecast_runs_after_coverage_and_keeps_failure_evidence():
    workflow_by_name = _workflow()
    test_steps = workflow_by_name["jobs"]["test"]["steps"]
    names = [step_by_name.get("name") for step_by_name in test_steps]

    assert names.index("Run tests") < names.index("Test coverage forecast")
    assert names.index("Test coverage forecast") < names.index(
        "Upload coverage evidence"
    )
    evidence_upload_step = next(
        step_by_name
        for step_by_name in test_steps
        if step_by_name.get("name") == "Upload coverage evidence"
    )
    forecast_upload_step = next(
        step_by_name
        for step_by_name in test_steps
        if step_by_name.get("name") == "Upload coverage forecast"
    )
    assert "test-coverage-python.json" in evidence_upload_step["with"]["path"]
    assert "coverage-provenance.json" in evidence_upload_step["with"]["path"]
    assert "coverage-forecast.json" in forecast_upload_step["with"]["path"]


def test_ci_uses_the_shared_prepush_families():
    jobs = _workflow()["jobs"]
    expected_mode_by_job = {
        "public-hygiene": ("Run public hygiene", "hygiene"),
        "python-quality": ("Run Python quality", "quality"),
        "security": ("Run dependency audits", "security"),
        "postgres18-db-tests": ("Run PostgreSQL checks", "postgres"),
        "redis-worker-smoke": ("Run Redis checks", "redis"),
    }

    for job_name, (step_name, mode) in expected_mode_by_job.items():
        step = _steps_by_name(jobs[job_name])[step_name]
        assert step["run"].strip() == f"scripts/ci/prepush {mode}"

    test_steps = _steps_by_name(jobs["test"])
    assert test_steps["Run tests"]["run"].strip() == "scripts/ci/prepush test"
    assert test_steps["Test coverage forecast"]["run"].strip() == "scripts/ci/prepush coverage"
    prepush = PREPUSH_PATH.read_text(encoding="utf-8")
    assert 'mode="${1:-all}"' in prepush
    assert "run_semgrep" in prepush
    assert "returntocorp/semgrep-agent@sha256:" in prepush
