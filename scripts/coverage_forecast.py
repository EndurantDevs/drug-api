#!/usr/bin/env python3
"""Run a provenance-bound, CI-equivalent single-report coverage forecast."""

from __future__ import annotations

import argparse
import hashlib
import json
from copy import deepcopy
from pathlib import Path
import subprocess
import sys
import tempfile
from typing import Any, Sequence

from coverage import __version__ as coverage_package_version

from coverage_forecast_reporting import build_forecast_diagnostics
from coverage_ratchet import _compare_baselines, _load_baseline
from coverage_reports import CoverageRatchetError, _collect_report


BASELINE_NAME = "test-coverage-baseline.json"
PROVENANCE_SCHEMA_VERSION = 1


class CoverageForecastError(ValueError):
    """Raised when forecast inputs cannot prove the CI measurement identity."""


def git_output(root: Path, *arguments: str) -> str:
    """Return one Git command's normalized output or a controlled error."""

    try:
        completed_process = subprocess.run(
            ["git", *arguments],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as error:
        raise CoverageForecastError(
            f"could not resolve coverage forecast Git identity: {' '.join(arguments)}"
        ) from error
    return completed_process.stdout.strip()


def resolve_forecast_base(root: Path, base_revision: str) -> tuple[str, str]:
    """Require the tested head to contain the exact requested target base."""

    if not base_revision:
        raise CoverageForecastError("coverage forecast requires a target base SHA")
    base_sha = git_output(root, "rev-parse", f"{base_revision}^{{commit}}")
    head_sha = git_output(root, "rev-parse", "HEAD")
    merge_base_sha = git_output(root, "merge-base", base_sha, head_sha)
    if merge_base_sha != base_sha:
        raise CoverageForecastError(
            "coverage forecast head does not contain the exact target base"
        )
    return base_sha, head_sha


def _sha256_file(path: Path) -> str:
    """Return the exact content digest of one coverage artifact."""

    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError as error:
        raise CoverageForecastError(f"missing coverage forecast file: {path}") from error


def _resolve_repository_report_path(root: Path, path: Path) -> Path:
    """Resolve a report path only when it is safely inside the repository."""

    candidate_path = path if path.is_absolute() else root / path
    try:
        resolved_path = candidate_path.resolve()
        resolved_path.relative_to(root.resolve())
    except ValueError as error:
        raise CoverageForecastError(
            f"coverage report must be inside the repository: {path}"
        ) from error
    return resolved_path


def _relative_repository_path(root: Path, path: Path) -> str:
    """Return the normalized repository-relative identity of one report."""

    return _resolve_repository_report_path(root, path).relative_to(
        root.resolve()
    ).as_posix()


def _write_json(path: Path, document_by_name: dict[str, Any]) -> None:
    """Write deterministic diagnostics or provenance, creating its output parent."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(document_by_name, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def write_coverage_provenance(
    root: Path,
    base_revision: str,
    report_path: Path,
    output_path: Path,
) -> dict[str, Any]:
    """Bind one generated coverage JSON report to the checkout and target base."""

    base_sha, head_sha = resolve_forecast_base(root, base_revision)
    resolved_report_path = _resolve_repository_report_path(root, report_path)
    relative_report_path = _relative_repository_path(root, resolved_report_path)
    document_by_name = {
        "schema_version": PROVENANCE_SCHEMA_VERSION,
        "base_sha": base_sha,
        "coverage_version": coverage_package_version,
        "head_sha": head_sha,
        "report_path": relative_report_path,
        "report_sha256": _sha256_file(resolved_report_path),
    }
    _write_json(output_path, document_by_name)
    return document_by_name


def _read_provenance(path: Path) -> dict[str, Any]:
    """Load a provenance document without allowing malformed JSON to escape."""

    try:
        document_by_name = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise CoverageForecastError(
            f"invalid coverage provenance: {path}"
        ) from error
    if not isinstance(document_by_name, dict):
        raise CoverageForecastError("coverage provenance must be a JSON object")
    return document_by_name


def verify_coverage_provenance(
    root: Path,
    base_sha: str,
    head_sha: str,
    report_path: Path,
    provenance_path: Path,
) -> None:
    """Fail closed when a report was generated for another base, head, or file."""

    resolved_report_path = _resolve_repository_report_path(root, report_path)
    provenance_by_name = _read_provenance(provenance_path)
    expected_by_name = {
        "schema_version": PROVENANCE_SCHEMA_VERSION,
        "base_sha": base_sha,
        "coverage_version": coverage_package_version,
        "head_sha": head_sha,
        "report_path": _relative_repository_path(root, resolved_report_path),
        "report_sha256": _sha256_file(resolved_report_path),
    }
    for field_name, expected_value in expected_by_name.items():
        if provenance_by_name.get(field_name) != expected_value:
            raise CoverageForecastError(
                f"coverage provenance {field_name} differs from the exact CI input"
            )


def _base_baseline(root: Path, base_sha: str, output_path: Path) -> dict[str, Any]:
    """Load the versioned ratchet baseline from exactly the requested base."""

    baseline_text = git_output(root, "show", f"{base_sha}:{BASELINE_NAME}")
    output_path.write_text(baseline_text + "\n", encoding="utf-8")
    return _load_baseline(output_path)


def _with_report_path(baseline_by_name: dict[str, Any], report_path: Path) -> dict[str, Any]:
    """Point a copied baseline at the actual CI report without mutating source data."""

    temporary_baseline_by_name = deepcopy(baseline_by_name)
    temporary_baseline_by_name["reports"]["python"]["path"] = str(
        report_path.resolve()
    )
    return temporary_baseline_by_name


def _with_report_snapshot(
    root: Path,
    baseline_by_name: dict[str, Any],
    report_path: Path,
) -> dict[str, Any]:
    """Stage one candidate baseline with the exact CI report snapshot."""

    temporary_baseline_by_name = _with_report_path(baseline_by_name, report_path)
    candidate_report_by_name = temporary_baseline_by_name["reports"]["python"]
    report_snapshot = _collect_report(root, "python", candidate_report_by_name)
    candidate_report_by_name["metrics"] = deepcopy(report_snapshot.metric_by_name)
    candidate_report_by_name["files"] = sorted(report_snapshot.files)
    return temporary_baseline_by_name


def _write_forecast_baselines(
    root: Path,
    temp_directory: Path,
    candidate_baseline: dict[str, Any],
    reference_baseline: dict[str, Any],
    report_path: Path,
) -> tuple[dict[str, Any], dict[str, Any], Path, Path]:
    """Stage one report snapshot for the candidate and base metrics for reference."""

    candidate_snapshot_baseline = _with_report_snapshot(
        root,
        candidate_baseline,
        report_path,
    )
    reference_report_baseline = _with_report_path(reference_baseline, report_path)
    candidate_path = temp_directory / "candidate-baseline.json"
    reference_path = temp_directory / "reference-baseline-report.json"
    _write_json(candidate_path, candidate_snapshot_baseline)
    _write_json(reference_path, reference_report_baseline)
    return (
        candidate_snapshot_baseline,
        reference_report_baseline,
        candidate_path,
        reference_path,
    )


def _run_ratchet(
    root: Path,
    candidate_path: Path,
    reference_path: Path,
    base_sha: str,
) -> subprocess.CompletedProcess[str]:
    """Run the exact production ratchet once against temporary immutable copies."""

    return subprocess.run(
        [
            sys.executable,
            "scripts/coverage_ratchet.py",
            "--baseline",
            str(candidate_path),
            "--reference-baseline",
            str(reference_path),
            "--changed-since",
            base_sha,
        ],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )


def _ratchet_errors(ratchet: subprocess.CompletedProcess[str]) -> list[str]:
    """Extract stable individual error messages from the existing gate output."""

    return [
        output_line.removeprefix("ERROR: ")
        for output_line in ratchet.stdout.splitlines()
        if output_line.startswith("ERROR: ")
    ]


def _print_forecast_summary(
    diagnostics_by_name: dict[str, Any],
    output_path: Path | None,
) -> None:
    """Print compact CI guidance while preserving detailed arcs in the artifact."""

    report_by_name = diagnostics_by_name["reports"]["python"]
    changed_line_count = report_by_name["changed_source_lines"]
    print(
        "coverage forecast: "
        f"base={diagnostics_by_name['base_sha']} "
        f"head={diagnostics_by_name['head_sha']} "
        f"changed_source_lines={changed_line_count}"
    )
    for metric_name, metric_by_name in report_by_name["metrics"].items():
        print(
            f"coverage forecast {metric_name}: "
            f"missing={metric_by_name['current_missing']} "
            f"cap={metric_by_name['effective_missing_cap']} "
            f"margin={metric_by_name['margin']}"
        )
    if output_path is not None:
        print(f"coverage forecast diagnostics: {output_path}")


def run_forecast(
    root: Path,
    base_revision: str,
    report_path: Path,
    provenance_path: Path,
    output_path: Path | None,
) -> int:
    """Verify one report, run the production ratchet, and emit useful diagnostics."""

    resolved_report_path = _resolve_repository_report_path(root, report_path)
    base_sha, head_sha = resolve_forecast_base(root, base_revision)
    verify_coverage_provenance(
        root,
        base_sha,
        head_sha,
        resolved_report_path,
        provenance_path,
    )
    candidate_baseline = _load_baseline(root / BASELINE_NAME)
    with tempfile.TemporaryDirectory(prefix="drug-api-coverage-forecast-") as raw_temp:
        temp_directory = Path(raw_temp)
        reference_baseline = _base_baseline(
            root,
            base_sha,
            temp_directory / "reference-baseline.json",
        )
        baseline_errors = _compare_baselines(candidate_baseline, reference_baseline)
        if baseline_errors:
            raise CoverageForecastError("; ".join(baseline_errors))
        (
            candidate_snapshot_baseline,
            reference_report_baseline,
            candidate_path,
            reference_path,
        ) = _write_forecast_baselines(
            root,
            temp_directory,
            candidate_baseline,
            reference_baseline,
            resolved_report_path,
        )
        ratchet = _run_ratchet(root, candidate_path, reference_path, base_sha)
        diagnostics_by_name = build_forecast_diagnostics(
            root,
            base_sha,
            head_sha,
            candidate_snapshot_baseline,
            reference_report_baseline,
            resolved_report_path,
        )
    diagnostics_by_name["ratchet_errors"] = _ratchet_errors(ratchet)
    diagnostics_by_name["ratchet_exit_code"] = ratchet.returncode
    if output_path is not None:
        _write_json(output_path, diagnostics_by_name)
    if ratchet.stdout:
        print(ratchet.stdout, end="")
    if ratchet.stderr:
        print(ratchet.stderr, file=sys.stderr, end="")
    _print_forecast_summary(diagnostics_by_name, output_path)
    return ratchet.returncode


def _parse_arguments(raw_arguments: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    provenance_parser = subparsers.add_parser("write-provenance")
    provenance_parser.add_argument("--base", required=True)
    provenance_parser.add_argument("--report", type=Path, required=True)
    provenance_parser.add_argument("--output", type=Path, required=True)
    forecast_parser = subparsers.add_parser("forecast")
    forecast_parser.add_argument("--base", required=True)
    forecast_parser.add_argument("--report", type=Path, required=True)
    forecast_parser.add_argument("--provenance", type=Path, required=True)
    forecast_parser.add_argument("--output", type=Path)
    return parser.parse_args(raw_arguments)


def _write_forecast_error(
    output_path: Path | None,
    error: CoverageForecastError | CoverageRatchetError,
) -> None:
    """Persist a controlled failure so the always-uploaded artifact explains it."""

    if output_path is not None:
        _write_json(output_path, {"schema_version": 1, "error": str(error)})


def main(raw_arguments: Sequence[str] | None = None) -> int:
    """Dispatch provenance writing or the strict forecast command."""

    arguments = _parse_arguments(raw_arguments)
    root = Path.cwd()
    try:
        if arguments.command == "write-provenance":
            write_coverage_provenance(
                root,
                arguments.base,
                arguments.report,
                arguments.output,
            )
            return 0
        return run_forecast(
            root,
            arguments.base,
            arguments.report,
            arguments.provenance,
            arguments.output,
        )
    except (CoverageForecastError, CoverageRatchetError) as error:
        _write_forecast_error(getattr(arguments, "output", None), error)
        print(f"ERROR: {error}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
