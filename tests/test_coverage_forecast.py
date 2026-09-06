"""Focused contracts for the single-report coverage forecast."""

from __future__ import annotations

import importlib
import json
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPTS_DIRECTORY = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS_DIRECTORY) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIRECTORY))

coverage_forecast = importlib.import_module("coverage_forecast")
coverage_forecast_reporting = importlib.import_module("coverage_forecast_reporting")
coverage_ratchet = importlib.import_module("coverage_ratchet")


BASE_SHA = "a" * 40
HEAD_SHA = "b" * 40


def _baseline(report_path: Path) -> dict:
    return {
        "schema_version": 1,
        "reports": {
            "python": {
                "format": "coverage.py",
                "path": str(report_path),
                "scope": {
                    "include": ["pkg/*.py"],
                    "exclude": [],
                    "policy": {
                        "branch": True,
                        "coverage": "7.15.2",
                        "pytest": "9.0.3",
                    },
                },
                "files": ["pkg/sample.py"],
                "metrics": {
                    "branches": {"covered": 3, "total": 5},
                    "lines": {"covered": 8, "total": 10},
                },
                "growth": {
                    "debt_reduction_percent": 0,
                    "diff_coverage_percent": 85,
                },
            }
        },
    }


def _write_report(
    root: Path,
    covered_count: int = 8,
    total_count: int = 10,
    branch_total: int = 10,
) -> Path:
    source_path = root / "pkg" / "sample.py"
    source_path.parent.mkdir()
    source_path.write_text("value = 1\n", encoding="utf-8")
    report_path = root / "test-coverage-python.json"
    report_path.write_text(
        json.dumps(
            {
                "files": {
                    str(source_path): {
                        "executed_lines": list(range(1, covered_count + 1)),
                        "missing_lines": list(range(covered_count + 1, total_count + 1)),
                        "summary": {
                            "covered_lines": covered_count,
                            "num_statements": total_count,
                            "covered_branches": covered_count,
                            "num_branches": branch_total,
                        },
                        "missing_branches": [[1, 2], [1, 3]],
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    return report_path


def _growth_baseline(report_path: Path) -> dict:
    """Return a static 80-percent reference floor."""

    baseline_by_name = _baseline(report_path)
    baseline_by_name["reports"]["python"]["metrics"] = {
        "branches": {"covered": 80, "total": 100},
        "lines": {"covered": 80, "total": 100},
    }
    return baseline_by_name


def _staged_ratchet_result(
    root: Path,
    candidate_path: Path,
    reference_path: Path,
    _base_sha: str,
) -> subprocess.CompletedProcess[str]:
    """Exercise production ratchet functions over forecast-staged baselines."""

    candidate_baseline = coverage_ratchet._load_baseline(candidate_path)
    reference_baseline = coverage_ratchet._load_baseline(reference_path)
    errors = coverage_ratchet._compare_baselines(
        candidate_baseline,
        reference_baseline,
    )
    errors.extend(
        coverage_ratchet._check_current_report(
            root,
            "python",
            candidate_baseline["reports"]["python"],
        )
    )
    stdout = "".join(f"ERROR: {error}\n" for error in errors)
    return subprocess.CompletedProcess(
        ["coverage_ratchet"],
        int(bool(errors)),
        stdout,
        "",
    )


def _run_growth_forecast(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    covered_count: int,
    baseline_output_path: Path | None = None,
) -> tuple[int, dict]:
    """Run forecast staging with the same report-driven gate used in CI."""

    report_path = _write_report(
        tmp_path,
        covered_count=covered_count,
        total_count=100,
        branch_total=100,
    )
    candidate_baseline = _growth_baseline(report_path)
    for metric_by_name in candidate_baseline["reports"]["python"]["metrics"].values():
        metric_by_name["covered"] = 70
    reference_baseline = _growth_baseline(report_path)
    output_path = tmp_path / "forecast.json"
    monkeypatch.setattr(
        coverage_forecast,
        "resolve_forecast_base",
        lambda *_arguments: (BASE_SHA, HEAD_SHA),
    )
    monkeypatch.setattr(
        coverage_forecast,
        "verify_coverage_provenance",
        lambda *_arguments: None,
    )
    monkeypatch.setattr(
        coverage_forecast,
        "_load_baseline",
        lambda *_arguments: json.loads(json.dumps(candidate_baseline)),
    )
    monkeypatch.setattr(
        coverage_forecast,
        "_base_baseline",
        lambda *_arguments: json.loads(json.dumps(reference_baseline)),
    )
    monkeypatch.setattr(coverage_forecast, "_run_ratchet", _staged_ratchet_result)
    monkeypatch.setattr(
        coverage_forecast_reporting,
        "collect_diff_coverage",
        lambda *_arguments: (
            {"python": {"changed": 17, "covered": 17, "total": 17, "threshold": 85}}, []
        ),
    )

    exit_code = coverage_forecast.run_forecast(
        tmp_path,
        BASE_SHA,
        report_path,
        tmp_path / "coverage-provenance.json",
        output_path,
        baseline_output_path=baseline_output_path,
    )
    return exit_code, json.loads(output_path.read_text(encoding="utf-8"))


def _provenance_by_name(report_path: Path) -> dict:
    return {
        "schema_version": coverage_forecast.PROVENANCE_SCHEMA_VERSION,
        "base_sha": BASE_SHA,
        "coverage_version": coverage_forecast.coverage_package_version,
        "head_sha": HEAD_SHA,
        "report_path": "test-coverage-python.json",
        "report_sha256": coverage_forecast._sha256_file(report_path),
    }


@pytest.mark.parametrize("field_name", ["base_sha", "head_sha"])
def test_provenance_rejects_source_or_target_base_drift(
    tmp_path: Path,
    field_name: str,
):
    report_path = _write_report(tmp_path)
    provenance_path = tmp_path / "coverage-provenance.json"
    provenance_by_name = _provenance_by_name(report_path)
    provenance_path.write_text(json.dumps(provenance_by_name), encoding="utf-8")

    coverage_forecast.verify_coverage_provenance(
        tmp_path,
        BASE_SHA,
        HEAD_SHA,
        report_path,
        provenance_path,
    )
    provenance_by_name[field_name] = "c" * 40
    provenance_path.write_text(json.dumps(provenance_by_name), encoding="utf-8")

    with pytest.raises(coverage_forecast.CoverageForecastError, match=field_name):
        coverage_forecast.verify_coverage_provenance(
            tmp_path,
            BASE_SHA,
            HEAD_SHA,
            report_path,
            provenance_path,
        )


def test_provenance_rejects_coverage_artifact_drift(tmp_path: Path):
    report_path = _write_report(tmp_path)
    provenance_path = tmp_path / "coverage-provenance.json"
    provenance_path.write_text(
        json.dumps(_provenance_by_name(report_path)),
        encoding="utf-8",
    )
    report_path.write_text("{}", encoding="utf-8")

    with pytest.raises(coverage_forecast.CoverageForecastError, match="report_sha256"):
        coverage_forecast.verify_coverage_provenance(
            tmp_path,
            BASE_SHA,
            HEAD_SHA,
            report_path,
            provenance_path,
        )


def test_forecast_refuses_a_head_without_the_exact_target_base(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    def fake_git_output(_root: Path, *arguments: str) -> str:
        output_by_arguments = {
            ("rev-parse", "base^{commit}"): BASE_SHA,
            ("rev-parse", "HEAD"): HEAD_SHA,
            ("merge-base", BASE_SHA, HEAD_SHA): "c" * 40,
        }
        return output_by_arguments[arguments]

    monkeypatch.setattr(coverage_forecast, "git_output", fake_git_output)

    with pytest.raises(
        coverage_forecast.CoverageForecastError,
        match="contain the exact target base",
    ):
        coverage_forecast.resolve_forecast_base(tmp_path, "base")


def test_forecast_stages_report_metrics_for_the_real_ratchet(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    """Stale tracked counts cannot reject genuinely improved measured coverage."""

    exit_code, forecast_by_name = _run_growth_forecast(
        tmp_path,
        monkeypatch,
        covered_count=81,
    )

    assert exit_code == 0
    assert forecast_by_name["ratchet_exit_code"] == 0
    assert forecast_by_name["ratchet_errors"] == []
    for metric_by_name in forecast_by_name["reports"]["python"]["metrics"].values():
        assert metric_by_name["current_missing"] == 19
        assert metric_by_name["ratio_delta"] == 1


def test_forecast_stages_report_files_without_changing_reference_policy(
    tmp_path: Path,
):
    """The candidate file set comes from the verified report, not tracked state."""

    report_path = _write_report(
        tmp_path,
        covered_count=8,
        total_count=10,
        branch_total=10,
    )
    extra_source_path = tmp_path / "pkg" / "extra.py"
    extra_source_path.write_text("extra = True\n", encoding="utf-8")
    report_by_name = json.loads(report_path.read_text(encoding="utf-8"))
    report_by_name["files"][str(extra_source_path)] = {
        "executed_lines": [1, 2],
        "missing_lines": [],
        "summary": {
            "covered_lines": 2,
            "num_statements": 2,
            "covered_branches": 2,
            "num_branches": 2,
        }
    }
    report_path.write_text(json.dumps(report_by_name), encoding="utf-8")
    candidate_baseline = _baseline(report_path)
    reference_baseline = _baseline(report_path)

    (
        candidate_snapshot,
        reference_snapshot,
        _candidate_path,
        _reference_path,
    ) = coverage_forecast._write_forecast_baselines(
        tmp_path,
        tmp_path,
        candidate_baseline,
        reference_baseline,
        report_path,
    )

    candidate_report = candidate_snapshot["reports"]["python"]
    reference_report = reference_snapshot["reports"]["python"]
    assert candidate_report["files"] == ["pkg/extra.py", "pkg/sample.py"]
    assert candidate_report["metrics"] == {
        "branches": {"covered": 10, "total": 12},
        "lines": {"covered": 10, "total": 12},
    }
    assert reference_report["files"] == ["pkg/sample.py"]
    assert reference_report["metrics"] == reference_baseline["reports"]["python"]["metrics"]
    assert reference_report["scope"] == reference_baseline["reports"]["python"]["scope"]


def test_forecast_keeps_the_ratchet_red_when_report_ratio_regresses(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    """Report-derived staging still rejects a genuine ratio regression."""

    exit_code, forecast_by_name = _run_growth_forecast(
        tmp_path,
        monkeypatch,
        covered_count=79,
    )

    assert exit_code == 1
    assert forecast_by_name["ratchet_exit_code"] == 1
    assert any(
        "coverage fell" in error
        for error in forecast_by_name["ratchet_errors"]
    )
    for metric_by_name in forecast_by_name["reports"]["python"]["metrics"].values():
        assert metric_by_name["current_missing"] == 21
        assert metric_by_name["ratio_delta"] == -1


@pytest.mark.parametrize("covered_count", [79, 81])
def test_forecast_publishes_machine_evidence_only_after_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    covered_count: int,
):
    docs_path = tmp_path / "docs"
    docs_path.mkdir()
    (docs_path / "test-coverage.md").write_text(
        "<!-- coverage-baseline:start -->\n<!-- coverage-baseline:end -->\n",
        encoding="utf-8",
    )
    baseline_output = tmp_path / "artifacts" / "test-coverage-baseline.json"
    exit_code, _diagnostics = _run_growth_forecast(
        tmp_path, monkeypatch, covered_count, baseline_output,
    )
    assert baseline_output.exists() is (exit_code == 0)
    assert baseline_output.with_name("test-coverage.md").exists() is (exit_code == 0)
    if exit_code == 0:
        measured_baseline = json.loads(baseline_output.read_text(encoding="utf-8"))
        assert measured_baseline["source_sha"] == HEAD_SHA
        assert measured_baseline["reports"]["python"]["metrics"]["lines"]["covered"] == covered_count


def test_forecast_error_writes_an_always_uploaded_artifact(tmp_path: Path):
    output_path = tmp_path / "forecast.json"
    failure = coverage_forecast.CoverageForecastError("provenance drift")

    coverage_forecast._write_forecast_error(output_path, failure)

    assert json.loads(output_path.read_text(encoding="utf-8")) == {
        "schema_version": 1,
        "error": "provenance drift",
    }
