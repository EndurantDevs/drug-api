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
    test_job_by_name = workflow_by_name["jobs"]["prepush"]
    steps_by_name = _steps_by_name(test_job_by_name)

    checkout_step = next(
        step_by_name
        for step_by_name in test_job_by_name["steps"]
        if step_by_name.get("uses")
        == "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1"
    )
    gate_step = steps_by_name["Run exact shared gate"]
    evidence_upload_step = steps_by_name["Upload exact-gate receipt and coverage evidence"]

    assert checkout_step["with"]["fetch-depth"] == 0
    assert checkout_step["with"]["ref"] == "${{ github.sha }}"
    assert "github.event.pull_request.base.sha || github.event.before" in str(
        gate_step["env"]
    )
    assert gate_step["run"] == "scripts/ci/prepush all"
    assert "HEAD^" not in str(gate_step)
    assert evidence_upload_step["if"] == "always()"


def test_ci_forecast_runs_after_coverage_and_keeps_failure_evidence():
    workflow_by_name = _workflow()
    test_steps = workflow_by_name["jobs"]["prepush"]["steps"]
    names = [step_by_name.get("name") for step_by_name in test_steps]

    assert names.index("Run exact shared gate") < names.index(
        "Upload exact-gate receipt and coverage evidence"
    )
    evidence_upload_step = next(
        step_by_name
        for step_by_name in test_steps
        if step_by_name.get("name")
        == "Upload exact-gate receipt and coverage evidence"
    )
    assert "test-coverage-python.json" in evidence_upload_step["with"]["path"]
    assert "coverage-provenance.json" in evidence_upload_step["with"]["path"]
    assert "coverage-forecast.json" in evidence_upload_step["with"]["path"]
    assert "coverage-margins.txt" in evidence_upload_step["with"]["path"]
