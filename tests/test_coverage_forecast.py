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
                    "changed_line_divisor": 10,
                    "debt_reduction_percent": 1,
                    "target_percent_by_metric": {"branches": 90, "lines": 95},
                },
            }
        },
    }


def _write_report(
    root: Path,
    covered_count: int = 8,
    total_count: int = 10,
    branch_total: int = 7,
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
    """Return one static baseline that requires a one-unit debt payment."""

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
        {"python": 17},
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
) -> tuple[int, dict]:
    """Run forecast staging with the same report-driven gate used in CI."""

    report_path = _write_report(
        tmp_path,
        covered_count=covered_count,
        total_count=100,
        branch_total=100,
    )
    candidate_baseline = _growth_baseline(report_path)
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
        "collect_growth_evidence",
        lambda *_arguments: ({"python": 17}, []),
    )

    exit_code = coverage_forecast.run_forecast(
        tmp_path,
        BASE_SHA,
        report_path,
        tmp_path / "coverage-provenance.json",
        output_path,
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
    """A static baseline cannot make a report under the cap fail the forecast."""

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
        assert metric_by_name["effective_missing_cap"] == 19
        assert metric_by_name["margin"] == 0


def test_forecast_keeps_the_ratchet_red_when_report_debt_misses_the_cap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    """Report-derived staging does not weaken a genuine debt-paydown failure."""

    exit_code, forecast_by_name = _run_growth_forecast(
        tmp_path,
        monkeypatch,
        covered_count=80,
    )

    assert exit_code == 1
    assert forecast_by_name["ratchet_exit_code"] == 1
    assert any(
        "uncovered debt must fall by 1 to 19 or less" in error
        for error in forecast_by_name["ratchet_errors"]
    )
    for metric_by_name in forecast_by_name["reports"]["python"]["metrics"].values():
        assert metric_by_name["current_missing"] == 20
        assert metric_by_name["effective_missing_cap"] == 19
        assert metric_by_name["margin"] == -1


def test_forecast_error_writes_an_always_uploaded_artifact(tmp_path: Path):
    output_path = tmp_path / "forecast.json"
    failure = coverage_forecast.CoverageForecastError("provenance drift")

    coverage_forecast._write_forecast_error(output_path, failure)

    assert json.loads(output_path.read_text(encoding="utf-8")) == {
        "schema_version": 1,
        "error": "provenance drift",
    }
