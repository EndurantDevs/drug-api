"""Create structured diagnostics for the single-report coverage forecast."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from coverage_growth import (
    calculate_required_debt_reduction,
    collect_growth_evidence,
    load_growth_policy,
)
from coverage_reports import (
    _collect_report,
    _is_path_in_scope,
    _read_json,
    _relative_report_path,
)


def _missing_branch_arcs(
    root: Path,
    report_path: Path,
    report_config_by_name: dict[str, Any],
) -> dict[str, list[list[int]]]:
    """Return normalized missing branch arcs for in-scope Python files."""

    report_document = _read_json(report_path)
    raw_files = report_document.get("files")
    if not isinstance(raw_files, dict):
        return {}
    missing_by_path: dict[str, list[list[int]]] = {}
    for raw_path, file_payload in raw_files.items():
        if not isinstance(raw_path, str) or not isinstance(file_payload, dict):
            continue
        relative_path = _relative_report_path(root, raw_path)
        if relative_path is None or not _is_path_in_scope(
            relative_path,
            report_config_by_name,
        ):
            continue
        raw_arcs = file_payload.get("missing_branches")
        normalized_arcs = _normalized_branch_arcs(raw_arcs)
        if normalized_arcs:
            missing_by_path[relative_path] = normalized_arcs
    return missing_by_path


def _normalized_branch_arcs(raw_arcs: Any) -> list[list[int]]:
    """Validate coverage.py branch arcs before including them in diagnostics."""

    if not isinstance(raw_arcs, list):
        return []
    normalized_arcs: list[list[int]] = []
    for raw_arc in raw_arcs:
        if (
            isinstance(raw_arc, list)
            and len(raw_arc) == 2
            and all(isinstance(line_number, int) for line_number in raw_arc)
        ):
            normalized_arcs.append(raw_arc)
    return sorted(normalized_arcs)


def _metric_diagnostics(
    current_metric_by_name: dict[str, int],
    base_metric_by_name: dict[str, int],
    target_percent: int,
    growth_policy_by_name: dict[str, Any],
    changed_line_count: int,
) -> dict[str, int]:
    """Return the exact debt cap and margin used by the ratchet policy."""

    current_missing_count = (
        current_metric_by_name["total"] - current_metric_by_name["covered"]
    )
    base_missing_count = (
        base_metric_by_name["total"] - base_metric_by_name["covered"]
    )
    is_at_target = (
        current_metric_by_name["covered"] * 100
        >= target_percent * current_metric_by_name["total"]
    )
    required_reduction = 0
    if not is_at_target and changed_line_count:
        required_reduction = calculate_required_debt_reduction(
            base_metric_by_name,
            target_percent,
            growth_policy_by_name["debt_reduction_percent"],
            changed_line_count,
            growth_policy_by_name["changed_line_divisor"],
        )
    effective_missing_cap = base_missing_count - required_reduction
    return {
        "base_missing": base_missing_count,
        "current_missing": current_missing_count,
        "effective_missing_cap": effective_missing_cap,
        "margin": effective_missing_cap - current_missing_count,
        "required_growth_reduction": required_reduction,
        "target_percent": target_percent,
    }


def build_forecast_diagnostics(
    root: Path,
    base_sha: str,
    head_sha: str,
    candidate_baseline: dict[str, Any],
    reference_baseline: dict[str, Any],
    report_path: Path,
) -> dict[str, Any]:
    """Build ratchet-equivalent metrics plus missing branches for one report."""

    report_name = "python"
    candidate_report = candidate_baseline["reports"][report_name]
    reference_report = reference_baseline["reports"][report_name]
    changed_by_report, exclusion_errors = collect_growth_evidence(
        root,
        base_sha,
        candidate_baseline,
        [report_name],
    )
    current_snapshot = _collect_report(root, report_name, candidate_report)
    growth_policy_by_name = load_growth_policy(report_name, candidate_report)
    metric_diagnostics_by_name: dict[str, dict[str, int]] = {}
    for metric_name, base_metric_by_name in reference_report["metrics"].items():
        current_metric_by_name = current_snapshot.metric_by_name[metric_name]
        metric_diagnostics_by_name[metric_name] = _metric_diagnostics(
            current_metric_by_name,
            base_metric_by_name,
            growth_policy_by_name["target_percent_by_metric"][metric_name],
            growth_policy_by_name,
            changed_by_report[report_name],
        )
    return {
        "schema_version": 1,
        "base_sha": base_sha,
        "head_sha": head_sha,
        "reports": {
            report_name: {
                "changed_source_lines": changed_by_report[report_name],
                "exclusion_errors": exclusion_errors,
                "metrics": metric_diagnostics_by_name,
                "missing_branch_arcs": _missing_branch_arcs(
                    root,
                    report_path,
                    candidate_report,
                ),
            }
        },
    }
