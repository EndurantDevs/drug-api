#!/usr/bin/env python3
"""Probe every Drug API OpenAPI operation and report aggregate latency."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import math
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OPENAPI_PATH = ROOT / "openapi.yaml"
HTTP_METHODS = {"delete", "get", "head", "options", "patch", "post", "put", "trace"}
MAX_PAGE = 0
MAX_RESULTS_PER_PAGE = 5
MAX_LIMIT = 5


@dataclass(frozen=True)
class ProbeCase:
    operation_id: str
    path: str
    expected_statuses: tuple[int, ...]


@dataclass(frozen=True)
class ProbeResult:
    operation_id: str
    status: int | None
    elapsed_ms: int
    ok: bool
    error: str | None = None


def load_spec(path: Path) -> dict[str, Any]:
    """Load the checked-in OpenAPI document."""
    return yaml.safe_load(path.read_text())


def resolve_parameter(spec: dict[str, Any], parameter: dict[str, Any]) -> dict[str, Any]:
    """Resolve a local reusable OpenAPI parameter."""
    reference = parameter.get("$ref")
    if not reference:
        return parameter
    prefix = "#/components/parameters/"
    if not str(reference).startswith(prefix):
        raise ValueError(f"unsupported parameter reference: {reference}")
    return spec["components"]["parameters"][str(reference)[len(prefix) :]]


def parameter_value(parameter: dict[str, Any]) -> Any:
    """Return the documented example or default for a parameter."""
    if "example" in parameter:
        return parameter["example"]
    examples = parameter.get("examples") or {}
    if examples:
        first = next(iter(examples.values()))
        return first.get("value") if isinstance(first, dict) and "value" in first else first
    schema = parameter.get("schema") or {}
    for key in ("example", "default"):
        if key in schema:
            return schema[key]
    return None


def operation_cases(spec: dict[str, Any]) -> list[ProbeCase]:
    """Build one request from every documented read-only operation."""
    cases: list[ProbeCase] = []
    for path, path_item in sorted((spec.get("paths") or {}).items()):
        shared_parameters = path_item.get("parameters") or []
        for method, operation in sorted(path_item.items()):
            if method not in HTTP_METHODS:
                continue
            if method != "get":
                raise ValueError(f"{method.upper()} {path} needs an explicit monitoring safety policy")
            cases.append(
                build_case(
                    spec,
                    path,
                    operation,
                    [*shared_parameters, *(operation.get("parameters") or [])],
                )
            )
    if not cases:
        raise ValueError("OpenAPI contract has no monitoring cases")
    return cases


def build_case(
    spec: dict[str, Any],
    path: str,
    operation: dict[str, Any],
    raw_parameters: list[dict[str, Any]],
) -> ProbeCase:
    """Materialize documented path and query parameter examples."""
    query_parameters: list[tuple[str, str]] = []
    rendered_path = path
    for raw_parameter in raw_parameters:
        parameter = resolve_parameter(spec, raw_parameter)
        name = str(parameter.get("name") or "")
        parameter_example = _bounded_parameter_value(name, parameter_value(parameter))
        location = parameter.get("in")
        if location == "path":
            if parameter_example is None:
                raise ValueError(f"GET {path} lacks an example for path parameter {name}")
            rendered_path = rendered_path.replace(
                "{" + name + "}", urllib.parse.quote(str(parameter_example), safe="")
            )
        elif location == "query" and parameter_example is not None:
            query_parameters.append((name, _query_text(parameter_example)))
        elif location == "query" and parameter.get("required"):
            raise ValueError(f"GET {path} lacks an example for query parameter {name}")
    if "{" in rendered_path or "}" in rendered_path:
        raise ValueError(f"unresolved path parameter for GET {path}")
    if query_parameters:
        rendered_path = f"{rendered_path}?{urllib.parse.urlencode(query_parameters)}"
    expected_statuses = tuple(
        sorted(
            int(status)
            for status in (operation.get("responses") or {})
            if str(status).isdigit()
        )
    )
    if not expected_statuses:
        raise ValueError(f"GET {path} has no explicit response status")
    return ProbeCase(
        operation_id=str(operation.get("operationId") or f"get_{path}"),
        path=rendered_path,
        expected_statuses=expected_statuses,
    )


def _query_text(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, list):
        return ",".join(str(item) for item in value)
    return str(value)


def _bounded_parameter_value(name: str, value: Any) -> Any:
    """Keep generated list and pagination requests small and deterministic."""
    if name == "page":
        return MAX_PAGE
    if name == "results_per_page":
        return min(_integer_value(value), MAX_RESULTS_PER_PAGE)
    if name == "limit":
        return min(_integer_value(value), MAX_LIMIT)
    return value


def _integer_value(value: Any) -> int:
    """Reject non-integer list bounds instead of generating an unsafe request."""
    try:
        return int(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"list bound must be an integer: {value!r}") from error


def execute_case(
    case: ProbeCase,
    *,
    base_url: str,
    api_key: str,
    timeout: float,
) -> ProbeResult:
    """Execute one GET without retaining response bodies."""
    headers_by_name = {"Accept": "application/json", "User-Agent": "HealthPortaMonitor/1.0"}
    if api_key:
        headers_by_name["Authorization"] = f"Bearer {api_key}"
    request = urllib.request.Request(
        base_url.rstrip("/") + case.path,
        headers=headers_by_name,
        method="GET",
    )
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            response.read(1)
            status = response.status
            error = None
    except urllib.error.HTTPError as exc:
        status = exc.code
        error = None
        exc.close()
    except (OSError, TimeoutError, urllib.error.URLError) as exc:
        status = None
        error = type(exc).__name__
    elapsed_ms = max(0, round((time.perf_counter() - started) * 1000))
    is_ok = status is not None and 200 <= status < 300
    return ProbeResult(case.operation_id, status, elapsed_ms, is_ok, error)


def percentile_95(values: list[int]) -> int:
    """Return the nearest-rank p95 for a non-empty sample."""
    if not values:
        raise ValueError("p95 requires at least one value")
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * 0.95) - 1)]


def run_cases(
    cases: list[ProbeCase],
    *,
    base_url: str,
    api_key: str,
    timeout: float,
    workers: int,
    max_p95_ms: int,
) -> dict[str, Any]:
    """Run all cases and return a redacted aggregate result."""
    if workers < 1 or timeout <= 0:
        raise ValueError("workers and timeout must be positive")
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        probe_results = list(
            executor.map(
                lambda case: execute_case(
                    case,
                    base_url=base_url,
                    api_key=api_key,
                    timeout=timeout,
                ),
                cases,
            )
        )
    p95_ms = percentile_95([probe_result.elapsed_ms for probe_result in probe_results])
    failures = [asdict(probe_result) for probe_result in probe_results if not probe_result.ok]
    is_latency_ok = max_p95_ms <= 0 or p95_ms <= max_p95_ms
    return {
        "ok": not failures and is_latency_ok,
        "operation_count": len(cases),
        "failure_count": len(failures),
        "p95_ms": p95_ms,
        "max_p95_ms": max_p95_ms,
        "failures": failures[:20],
    }


def push_summary(push_url: str, summary: dict[str, Any]) -> None:
    """Publish the aggregate result to an Uptime Kuma Push monitor."""
    parsed_url = urllib.parse.urlsplit(push_url)
    if parsed_url.query or parsed_url.fragment:
        raise ValueError("Kuma push URL must not contain a query or fragment")
    message = (
        f"operations={summary['operation_count']} failures={summary['failure_count']} "
        f"p95_ms={summary['p95_ms']}"
    )
    separator = "&" if "?" in push_url else "?"
    url = push_url + separator + urllib.parse.urlencode(
        {
            "status": "up" if summary["ok"] else "down",
            "msg": message,
            "ping": summary["p95_ms"],
        }
    )
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            if response.status >= 300:
                raise RuntimeError("Kuma push failed")
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        if isinstance(exc, urllib.error.HTTPError):
            exc.close()
        raise RuntimeError("Kuma push failed") from None


def parse_args() -> argparse.Namespace:
    """Parse monitor configuration from flags and environment variables."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--openapi", type=Path, default=DEFAULT_OPENAPI_PATH)
    parser.add_argument("--base-url", default=os.getenv("MONITOR_BASE_URL", ""))
    parser.add_argument("--api-key", default=os.getenv("MONITOR_API_KEY", ""))
    parser.add_argument("--push-url", default=os.getenv("KUMA_PUSH_URL", ""))
    parser.add_argument("--timeout", type=float, default=2.0)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--max-p95-ms", type=int, default=0)
    parser.add_argument("--check", action="store_true")
    return parser.parse_args()


def main() -> int:
    """Check the OpenAPI contract or publish one aggregate probe result."""
    args = parse_args()
    cases = operation_cases(load_spec(args.openapi))
    if args.check:
        print(json.dumps({"safe_operation_count": len(cases)}, sort_keys=True))
        return 0
    if not args.base_url:
        raise SystemExit("--base-url or MONITOR_BASE_URL is required")
    summary = run_cases(
        cases,
        base_url=args.base_url,
        api_key=args.api_key,
        timeout=args.timeout,
        workers=args.workers,
        max_p95_ms=args.max_p95_ms,
    )
    print(json.dumps(summary, sort_keys=True))
    if args.push_url:
        push_summary(args.push_url, summary)
    return 0 if summary["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
