"""Workflow contracts for the single-report coverage forecast."""

from __future__ import annotations

from pathlib import Path

import yaml

WORKFLOW_PATH = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "ci.yml"


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
    provenance_step = steps_by_name["Write coverage forecast provenance"]
    forecast_step = steps_by_name["Test coverage forecast"]
    evidence_upload_step = steps_by_name["Upload coverage evidence"]
    forecast_upload_step = steps_by_name["Upload coverage forecast"]

    assert checkout_step["with"]["fetch-depth"] == 0
    assert checkout_step["with"]["ref"] == "${{ github.sha }}"
    assert "github.event.pull_request.base.sha || github.event.before" in str(
        provenance_step["env"]
    )
    assert "write-provenance" in provenance_step["run"]
    assert "forecast" in forecast_step["run"]
    assert "--provenance coverage-provenance.json" in forecast_step["run"]
    assert "HEAD^" not in forecast_step["run"]
    assert forecast_step["if"] == "always()"
    assert evidence_upload_step["if"] == "always()"
    assert forecast_upload_step["if"] == "always()"


def test_ci_forecast_runs_after_coverage_and_keeps_failure_evidence():
    workflow_by_name = _workflow()
    test_steps = workflow_by_name["jobs"]["test"]["steps"]
    names = [step_by_name.get("name") for step_by_name in test_steps]

    assert names.index("Run tests") < names.index("Write coverage report")
    assert names.index("Write coverage report") < names.index(
        "Write coverage forecast provenance"
    )
    assert names.index("Write coverage forecast provenance") < names.index(
        "Test coverage forecast"
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
