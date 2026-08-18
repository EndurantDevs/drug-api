#!/usr/bin/env python3
"""Probe each reviewed, bounded Drug API OpenAPI operation."""

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
MAX_REPORTED_FAILURES = 20
MAX_RESPONSE_BYTES = 65_536


@dataclass(frozen=True)
class ProbeCase:
    operation_id: str
    path: str | None
    expected_statuses: tuple[int, ...]
    max_latency_ms: int | None
    exclusion_reason: str | None = None


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
    """Discover every operation, materializing only explicitly bounded GETs."""
    cases: list[ProbeCase] = []
    for path, path_item in sorted((spec.get("paths") or {}).items()):
        shared_parameters = path_item.get("parameters") or []
        for method, operation in sorted(path_item.items()):
            if method not in HTTP_METHODS:
                continue
            policy = operation.get("x-monitor")
            mode = policy.get("mode") if isinstance(policy, dict) else None
            operation_id = str(operation.get("operationId") or f"{method}_{path}")
            if mode == "excluded":
                reason = policy.get("reason")
                if not isinstance(reason, str) or not reason:
                    raise ValueError(f"{method.upper()} {path} exclusion needs a reason")
                cases.append(ProbeCase(operation_id, None, (), None, reason))
            elif mode == "bounded":
                if method != "get":
                    raise ValueError(f"{method.upper()} {path}: only GET operations may be monitored live")
                max_latency_ms = policy.get("max_latency_ms")
                if isinstance(max_latency_ms, bool) or not isinstance(max_latency_ms, int) or max_latency_ms <= 0:
                    raise ValueError(f"GET {path} needs a positive x-monitor.max_latency_ms")
                cases.append(
                    build_case(
                        spec,
                        path,
                        operation,
                        [*shared_parameters, *(operation.get("parameters") or [])],
                        max_latency_ms,
                    )
                )
            else:
                raise ValueError(f"{method.upper()} {path} needs an explicit monitoring policy")
    if not cases:
        raise ValueError("OpenAPI contract has no monitoring cases")
    return cases


def build_case(
    spec: dict[str, Any],
    path: str,
    operation: dict[str, Any],
    raw_parameters: list[dict[str, Any]],
    max_latency_ms: int,
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
            if str(status).isdigit() and 200 <= int(status) < 300
        )
    )
    if not expected_statuses:
        raise ValueError(f"GET {path} has no explicit response status")
    return ProbeCase(
        operation_id=str(operation.get("operationId") or f"get_{path}"),
        path=rendered_path,
        expected_statuses=expected_statuses,
        max_latency_ms=max_latency_ms,
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


class NoRedirect(urllib.request.HTTPRedirectHandler):
    """Return redirects to the probe instead of issuing a second request."""

    def redirect_request(self, *_args: Any, **_kwargs: Any) -> None:
        return None


def open_no_redirect(request: urllib.request.Request, timeout: float):
    """Open one request without redirect handling."""
    return urllib.request.build_opener(NoRedirect()).open(request, timeout=timeout)


def request_url(base_url: str, case_path: str) -> str:
    """Return a URL constrained to the configured service origin."""
    parsed_base = urllib.parse.urlsplit(base_url)
    if (
        parsed_base.scheme not in {"http", "https"}
        or not parsed_base.netloc
        or parsed_base.query
        or parsed_base.fragment
        or not case_path.startswith("/")
        or case_path.startswith("//")
    ):
        raise ValueError("monitor case path must stay on the configured origin")
    url = base_url.rstrip("/") + case_path
    parsed_url = urllib.parse.urlsplit(url)
    if (parsed_url.scheme, parsed_url.netloc) != (parsed_base.scheme, parsed_base.netloc):
        raise ValueError("monitor case path must stay on the configured origin")
    return url


def response_error(response_body: bytes, content_type: str) -> str | None:
    """Return an error for a non-JSON or oversized successful response."""
    if "application/json" not in content_type.lower() and "+json" not in content_type.lower():
        return "invalid_content_type"
    if len(response_body) > MAX_RESPONSE_BYTES:
        return "response_too_large"
    try:
        json.loads(response_body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return "invalid_json"
    return None


def execute_case(
    case: ProbeCase,
    *,
    base_url: str,
    timeout: float,
) -> ProbeResult:
    """Execute one GET without retaining response bodies."""
    headers_by_name = {"Accept": "application/json", "User-Agent": "HealthPortaMonitor/1.0"}
    if case.path is None:
        raise ValueError(f"{case.operation_id} is excluded from live monitoring")
    request = urllib.request.Request(
        request_url(base_url, case.path),
        headers=headers_by_name,
        method="GET",
    )
    started = time.perf_counter()
    try:
        with open_no_redirect(request, timeout) as response:
            response_body = response.read(MAX_RESPONSE_BYTES + 1)
            status = response.status
            content_type = str(response.headers.get("Content-Type", ""))
            error = response_error(response_body, content_type)
    except urllib.error.HTTPError as exc:
        status = exc.code
        error = f"HTTP {status}"
        exc.close()
    except (OSError, TimeoutError, urllib.error.URLError) as exc:
        status = None
        error = type(exc).__name__
    elapsed_ms = max(0, round((time.perf_counter() - started) * 1000))
    is_ok = (
        status is not None
        and 200 <= status < 300
        and status in case.expected_statuses
        and error is None
    )
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
    timeout: float,
    workers: int,
) -> dict[str, Any]:
    """Run all cases and return a redacted aggregate result."""
    if workers < 1 or timeout <= 0:
        raise ValueError("workers and timeout must be positive")
    live_cases = [case for case in cases if case.path is not None]
    if not live_cases:
        raise ValueError("OpenAPI contract has no bounded live monitoring cases")
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        probe_results = list(
            executor.map(
                lambda case: execute_case(
                    case,
                    base_url=base_url,
                    timeout=timeout,
                ),
                live_cases,
            )
        )
    p95_ms = percentile_95([probe_result.elapsed_ms for probe_result in probe_results])
    all_failures = []
    for case, probe_result in zip(live_cases, probe_results):
        if probe_result.ok and probe_result.elapsed_ms <= case.max_latency_ms:
            continue
        failure = asdict(probe_result)
        failure["ok"] = False
        failure["max_latency_ms"] = case.max_latency_ms
        if probe_result.ok:
            failure["error"] = "latency budget exceeded"
        all_failures.append(failure)
    failures = all_failures[:MAX_REPORTED_FAILURES]
    return {
        "ok": not all_failures,
        "operation_count": len(cases),
        "probed_operation_count": len(live_cases),
        "excluded_operation_count": len(cases) - len(live_cases),
        "failure_count": len(all_failures),
        "p95_ms": p95_ms,
        "first_failure_operation_id": all_failures[0]["operation_id"] if all_failures else None,
        "failure_truncation_count": len(all_failures) - len(failures),
        "failures": failures,
    }


def push_summary(push_url: str, summary: dict[str, Any]) -> None:
    """Publish the aggregate result to an Uptime Kuma Push monitor."""
    parsed_url = urllib.parse.urlsplit(push_url)
    if (
        parsed_url.scheme not in {"http", "https"}
        or not parsed_url.netloc
        or parsed_url.query
        or parsed_url.fragment
    ):
        raise ValueError("Kuma push URL must be a bare HTTP(S) URL")
    message = (
        f"operations={summary['operation_count']} failures={summary['failure_count']} "
        f"p95_ms={summary['p95_ms']} first_failure={summary['first_failure_operation_id'] or '-'} "
        f"failure_truncated={summary['failure_truncation_count']}"
    )
    if summary.get("failures"):
        first_failure = summary["failures"][0]
        status = first_failure.get("status")
        error = first_failure.get("error") or "-"
        message += (
            f" status={status if status is not None else '-'}"
            f" error={error}"
            f" elapsed_ms={first_failure['elapsed_ms']}"
            f" budget_ms={first_failure['max_latency_ms']}"
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
        with open_no_redirect(urllib.request.Request(url, method="GET"), 10) as response:
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
    parser.add_argument("--push-url", default=os.getenv("KUMA_PUSH_URL", ""))
    parser.add_argument("--timeout", type=float, default=2.0)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--check", action="store_true")
    return parser.parse_args()


def main() -> int:
    """Check the OpenAPI contract or publish one aggregate probe result."""
    args = parse_args()
    cases = operation_cases(load_spec(args.openapi))
    if args.check:
        print(
            json.dumps(
                {
                    "contract_operation_count": len(cases),
                    "excluded_operation_count": sum(case.path is None for case in cases),
                    "safe_operation_count": sum(case.path is not None for case in cases),
                },
                sort_keys=True,
            )
        )
        return 0
    if not args.base_url:
        raise SystemExit("--base-url or MONITOR_BASE_URL is required")
    summary = run_cases(
        cases,
        base_url=args.base_url,
        timeout=args.timeout,
        workers=args.workers,
    )
    print(json.dumps(summary, sort_keys=True))
    if args.push_url:
        push_summary(args.push_url, summary)
    return 0 if summary["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
