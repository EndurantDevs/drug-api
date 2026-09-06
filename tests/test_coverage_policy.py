"""Focused transition checks; no services or repository gate execution."""

import argparse
import copy
import importlib
import json
import runpy
import subprocess
import sys
from pathlib import Path

import coverage
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

forecast = importlib.import_module("coverage_forecast")
growth = importlib.import_module("coverage_growth")
ratchet = importlib.import_module("coverage_ratchet")
reports = importlib.import_module("coverage_reports")
readability = importlib.import_module("readability.cli")


def test_equal_ratio_growth_preserves_scope_and_deselection_guards():
    baseline = growth.build_diff_policy_test_baseline()
    candidate = copy.deepcopy(baseline)
    candidate["reports"]["python"]["metrics"]["lines"] = {"covered": 160, "total": 200}
    assert ratchet._compare_baselines(candidate, baseline) == []
    candidate["reports"]["python"]["metrics"]["lines"]["covered"] = 159
    assert any("coverage fell" in error for error in ratchet._compare_baselines(candidate, baseline))
    candidate = copy.deepcopy(baseline)
    candidate["reports"]["python"]["files"] = []
    assert any("source files" in error for error in ratchet._compare_baselines(candidate, baseline))
    candidate = copy.deepcopy(baseline)
    candidate["reports"]["python"]["scope"]["policy"]["test_deselections"] = ["test_product"]
    assert any("deselections" in error for error in ratchet._compare_baselines(candidate, baseline))


def test_machine_reference_requires_exact_source_and_policy(tmp_path):
    tracked = growth.build_diff_policy_test_baseline()
    tracked.update(schema_version=1, machine_artifact_required=True)
    with pytest.raises(forecast.CoverageForecastError, match="requires its 90-day"):
        forecast._reference_baseline(tracked, "a" * 40, None)
    measured = copy.deepcopy(tracked)
    measured["source_sha"] = "a" * 40
    artifact_path = tmp_path / "baseline.json"
    artifact_path.write_text(json.dumps(measured))
    assert forecast._reference_baseline(tracked, "a" * 40, artifact_path) == measured
    measured["source_sha"] = "b" * 40
    artifact_path.write_text(json.dumps(measured))
    with pytest.raises(forecast.CoverageForecastError, match="source_sha"):
        forecast._reference_baseline(tracked, "a" * 40, artifact_path)
    measured["source_sha"] = "a" * 40
    measured["reports"]["python"]["scope"]["exclude"] = ["sample.py"]
    artifact_path.write_text(json.dumps(measured))
    with pytest.raises(forecast.CoverageForecastError, match="scope differs"):
        forecast._reference_baseline(tracked, "a" * 40, artifact_path)
    tracked.pop("machine_artifact_required")
    assert forecast._reference_baseline(tracked, "a" * 40, None) == tracked


def test_machine_requirement_and_report_identity_cannot_be_removed(tmp_path):
    baseline = growth.build_diff_policy_test_baseline()
    baseline["machine_artifact_required"] = True
    candidate = copy.deepcopy(baseline)
    candidate.pop("machine_artifact_required")
    assert any("requirement was removed" in error for error in ratchet._compare_baselines(candidate, baseline))
    candidate["reports"]["python"]["path"] = "changed.json"
    with pytest.raises(forecast.CoverageForecastError, match="baseline path changed"):
        forecast._write_forecast_baselines(tmp_path, tmp_path, candidate, baseline, tmp_path / "missing.json")


@pytest.mark.parametrize("field, value", [("metrics", None), ("metrics", {}),
                                       ("files", None), ("files", "sample.py"), ("files", [None])])
def test_malformed_tracked_report_produces_forecast_diagnostics(tmp_path, monkeypatch, field, value):
    tracked = growth.build_diff_policy_test_baseline()["reports"]["python"]
    measured = copy.deepcopy(tracked)
    tracked[field] = value

    def reject_report(*_):
        forecast._require_artifact_report("python", measured, tracked)

    monkeypatch.setattr(forecast, "run_forecast", reject_report)
    output = tmp_path / "forecast.json"
    baseline_output = tmp_path / "machine.json"
    assert forecast.main(["forecast", "--base", "a" * 40, "--report", "unused.json",
                          "--provenance", "unused-provenance.json", "--output", str(output),
                          "--baseline-output", str(baseline_output)]) == 2
    assert "malformed" in output.read_text()
    assert not baseline_output.exists()


@pytest.mark.parametrize("line_record", [
    {"executed_lines": [], "missing_lines": [], "summary": {"covered_lines": 1, "num_statements": 1}},
    {"executed_lines": [1, 1], "missing_lines": []},
    {"executed_lines": [1], "missing_lines": [1]},
    {"executed_lines": [True], "missing_lines": []},
    {"executed_lines": [1], "missing_lines": [], "summary": {"covered_lines": 2, "num_statements": 2}},
    {"executed_lines": [1], "missing_lines": [], "summary": []},
    {"executed_lines": [1], "missing_lines": [], "excluded_lines": None},
    {"executed_lines": [1], "missing_lines": [2], "excluded_lines": [2]},
])
def test_malformed_line_records_fail_closed(line_record):
    with pytest.raises(reports.CoverageRatchetError):
        growth._coveragepy_line_sets(line_record, "sample.py")


def test_real_coverage_excluded_execution_is_not_a_statement(tmp_path):
    sample = tmp_path / "sample.py"
    sample.write_text("TYPE_CHECKING = True\nif TYPE_CHECKING:\n    excluded = 1\ncovered = 2\n")
    report_path = tmp_path / "coverage.json"
    measured = coverage.Coverage(data_file=None, config_file=False)
    measured.start()
    try:
        runpy.run_path(str(sample))
    finally:
        measured.stop()
    measured.json_report(morfs=[str(sample)], outfile=str(report_path))
    payload = next(iter(json.loads(report_path.read_text())["files"].values()))
    assert payload["excluded_lines"] == [2, 3]
    assert growth._coveragepy_line_sets(payload, "sample.py") == ({1, 4}, {1, 4})
    # Older Coverage.py tracers also record execution of the excluded block.
    payload["executed_lines"] = [1, 2, 3, 4]
    assert growth._coveragepy_line_sets(payload, "sample.py") == ({1, 4}, {1, 4})
    config = growth.build_diff_policy_test_baseline()["reports"]["python"]
    result = growth._report_diff_coverage(tmp_path, "python", config, {"sample.py": {3}})
    assert (result["covered"], result["total"]) == (0, 0)
    growth.run_exclusion_guard_self_test()


def test_real_gate_enforces_changed_line_boundary(tmp_path, monkeypatch, capsys):
    (tmp_path / "sample.py").write_text("answer = 1\n" * 20)
    baseline = growth.build_diff_policy_test_baseline()
    baseline["schema_version"] = 1
    candidate = copy.deepcopy(baseline)
    candidate["reports"]["python"]["metrics"]["lines"] = {"covered": 16, "total": 20}
    (tmp_path / "baseline.json").write_text(json.dumps(candidate))
    (tmp_path / "reference.json").write_text(json.dumps(baseline))
    coverage_by_field = {"files": {"sample.py": {
        "summary": {"covered_lines": 17, "num_statements": 20, "covered_branches": 0, "num_branches": 0},
        "executed_lines": list(range(1, 18)), "missing_lines": [18, 19, 20],
    }}}
    report_path = tmp_path / "coverage.json"
    report_path.write_text(json.dumps(coverage_by_field))
    monkeypatch.setattr(growth, "_git_diff", lambda *_: (
        "diff --git a/sample.py b/sample.py\n+++ b/sample.py\n@@ -0,0 +1,20 @@\n" + "+answer = 1\n" * 20
    ))
    monkeypatch.chdir(tmp_path)
    args = argparse.Namespace(baseline="baseline.json", reference_baseline="reference.json",
                              changed_since="base", report_names=None, write_baseline=False)
    assert ratchet._execute_gate(args) == 0
    coverage_by_field["files"]["sample.py"].update(
        executed_lines=list(range(1, 17)), missing_lines=[17, 18, 19, 20],
    )
    coverage_by_field["files"]["sample.py"]["summary"]["covered_lines"] = 16
    report_path.write_text(json.dumps(coverage_by_field))
    assert ratchet._execute_gate(args) == 1
    assert "ERROR: python: diff coverage 80.00% is below 85%" in capsys.readouterr().out
    with pytest.raises(reports.CoverageRatchetError, match="absent from coverage"):
        growth._report_diff_coverage(tmp_path, "python", candidate["reports"]["python"],
                                     {"missing.py": {1}})


def test_machine_snapshot_has_matching_generated_docs(tmp_path):
    configured = ratchet._load_baseline(ROOT / "test-coverage-baseline.json")
    measured = copy.deepcopy(configured)
    measured["reports"]["python"]["path"] = "/temporary/report.json"
    measured["reports"]["python"]["metrics"] = {
        "lines": {"covered": 17, "total": 20},
        "branches": {"covered": 3, "total": 5},
    }
    output = tmp_path / "machine.json"
    forecast._write_measured_baseline(ROOT, output, measured, configured, "a" * 40)
    artifact = json.loads(output.read_text())
    assert artifact["source_sha"] == "a" * 40
    assert artifact["reports"]["python"]["path"] == configured["reports"]["python"]["path"]
    assert artifact["reports"]["python"]["metrics"] == measured["reports"]["python"]["metrics"]
    reports.check_baseline_docs(tmp_path / "test-coverage.md", artifact)
    reports.check_baseline_docs(ROOT / "docs/test-coverage.md", configured)


def test_cli_rejects_unknown_report_before_diff_scan(tmp_path, monkeypatch):
    baseline = growth.build_diff_policy_test_baseline()
    baseline["schema_version"] = 1
    (tmp_path / "baseline.json").write_text(json.dumps(baseline))
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(ratchet, "collect_diff_coverage", lambda *_: pytest.fail("diff scan reached"))
    args = argparse.Namespace(baseline="baseline.json", reference_baseline="unused.json",
                              changed_since="base", report_names=["unknown"], write_baseline=False)
    with pytest.raises(reports.CoverageRatchetError, match="unknown baseline reports: unknown"):
        ratchet._execute_gate(args)


def _commit_fixture(root):
    subprocess.run(["git", "-c", "core.hooksPath=/dev/null", "-c", "commit.gpgsign=false",
                    "-c", "user.name=Policy Test", "-c", "user.email=policy@example.invalid",
                    "commit", "-qam", "fixture"], cwd=root, check=True)


def test_diff_paths_ignore_user_noprefix_setting(tmp_path, monkeypatch):
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    subprocess.run(["git", "config", "diff.noprefix", "true"], cwd=tmp_path, check=True)
    sample = tmp_path / "café.py"
    sample.write_text("answer = 1\n")
    subprocess.run(["git", "add", "café.py"], cwd=tmp_path, check=True)
    _commit_fixture(tmp_path)
    base_sha = subprocess.run(["git", "rev-parse", "HEAD"], cwd=tmp_path, check=True,
                              capture_output=True, text=True).stdout.strip()
    sample.write_text("answer = 2\n")
    _commit_fixture(tmp_path)
    monkeypatch.setattr(subprocess, "_text_encoding", lambda: "ascii")
    assert growth.changed_lines_from_diff(growth._git_diff(tmp_path, base_sha)) == {"café.py": {1}}


def test_soft_length_and_renamed_huge_file_growth(tmp_path):
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    module = tmp_path / "module.py"
    module.write_text("# product\n" * 6)
    subprocess.run(["git", "add", "module.py"], cwd=tmp_path, check=True)
    _commit_fixture(tmp_path)
    base_sha = subprocess.run(["git", "rev-parse", "HEAD"], cwd=tmp_path, check=True,
                              capture_output=True, text=True).stdout.strip()
    config_by_field = {"source_roots": ["module.py", "renamed.py", "tests"],
                       "include_suffixes": [".py"], "thresholds": {"max_file_lines": 3, "huge_file_lines": 5},
                       "readability": {"file_length_roots": ["module.py", "renamed.py"]}}
    snapshot = readability.build_snapshot(tmp_path, config_by_field, base_sha)
    assert snapshot["issues"]["long_files"]
    assert readability._new_issues(snapshot, {}) == {}
    subprocess.run(["git", "mv", "module.py", "renamed.py"], cwd=tmp_path, check=True)
    (tmp_path / "renamed.py").write_text("# product\n" * 7)
    _commit_fixture(tmp_path)
    assert readability.build_snapshot(tmp_path, config_by_field, base_sha)["issues"]["huge_file_growth"]
    assert readability.build_snapshot(tmp_path, config_by_field, "missing")["issues"]["huge_file_growth"]
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "fixture.py").write_text("# fixture\n" * 8)
    snapshot = readability.build_snapshot(tmp_path, config_by_field)
    assert {issue["path"] for issue in snapshot["issues"]["long_files"]} == {"renamed.py"}
