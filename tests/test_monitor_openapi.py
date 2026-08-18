"""Contracts for the bounded live Drug API monitor."""

import urllib.parse

import pytest

from scripts import monitor_openapi


def test_monitor_cases_cover_every_openapi_operation() -> None:
    spec = monitor_openapi.load_spec(monitor_openapi.DEFAULT_OPENAPI_PATH)
    cases = monitor_openapi.operation_cases(spec)
    operation_count = sum(
        1
        for path_item in spec["paths"].values()
        for method in path_item
        if method in monitor_openapi.HTTP_METHODS
    )

    assert len(cases) == operation_count == 25
    assert sum(case.path is not None for case in cases) == 13
    assert sum(case.path is None for case in cases) == 12
    assert all("{" not in case.path for case in cases if case.path is not None)
    assert next(case for case in cases if case.operation_id == "listProductsAll").path is None


def test_monitor_cases_require_explicit_bounded_or_excluded_policy() -> None:
    spec_by_path = {
        "paths": {
            "/unclassified": {
                "get": {
                    "operationId": "unclassified",
                    "responses": {"200": {"description": "ok"}},
                }
            }
        }
    }

    with pytest.raises(ValueError, match="explicit monitoring policy"):
        monitor_openapi.operation_cases(spec_by_path)


@pytest.mark.parametrize("method", sorted(monitor_openapi.HTTP_METHODS - {"get"}))
def test_monitor_cases_fail_closed_for_non_get_operation(method: str) -> None:
    spec_by_path = {
        "paths": {
            "/unsafe": {
                method: {
                    "operationId": "unsafeMutation",
                    "x-monitor": {"mode": "bounded", "max_latency_ms": 2000},
                    "responses": {"204": {"description": "done"}},
                }
            }
        }
    }

    try:
        monitor_openapi.operation_cases(spec_by_path)
    except ValueError as exc:
        assert "only GET operations may be monitored live" in str(exc)
    else:
        raise AssertionError("non-GET operation was accepted")


def test_run_cases_reports_server_error_and_latency(monkeypatch) -> None:
    cases = [
        monitor_openapi.ProbeCase("healthy", "/healthy", (200,), 100),
        monitor_openapi.ProbeCase("unready", "/ready", (200, 503), 100),
    ]

    def fake_execute(case, **_kwargs):
        if case.operation_id == "healthy":
            return monitor_openapi.ProbeResult("healthy", 200, 40, True)
        return monitor_openapi.ProbeResult("unready", 503, 240, False)

    monkeypatch.setattr(monitor_openapi, "execute_case", fake_execute)
    summary = monitor_openapi.run_cases(
        cases,
        base_url="http://monitor.invalid",
        timeout=1,
        workers=1,
    )

    assert summary["ok"] is False
    assert summary["operation_count"] == 2
    assert summary["failure_count"] == 1
    assert summary["p95_ms"] == 240
    assert summary["first_failure_operation_id"] == "unready"
    assert summary["failure_truncation_count"] == 0


def test_run_cases_fails_an_individual_latency_budget(monkeypatch) -> None:
    case = monitor_openapi.ProbeCase("slow", "/slow", (200,), 100)
    monkeypatch.setattr(
        monitor_openapi,
        "execute_case",
        lambda *_args, **_kwargs: monitor_openapi.ProbeResult("slow", 200, 101, True),
    )

    summary = monitor_openapi.run_cases(
        [case],
        base_url="http://monitor.invalid",
        timeout=1,
        workers=1,
    )

    assert summary["ok"] is False
    assert summary["p95_ms"] == 101
    assert summary["failures"] == [
        {
            "operation_id": "slow",
            "status": 200,
            "elapsed_ms": 101,
            "ok": False,
            "error": "latency budget exceeded",
            "max_latency_ms": 100,
        }
    ]


def test_run_cases_reports_first_failure_and_truncation(monkeypatch) -> None:
    cases = [monitor_openapi.ProbeCase(f"failed-{index}", "/failed", (200,), 100) for index in range(21)]
    monkeypatch.setattr(
        monitor_openapi,
        "execute_case",
        lambda case, **_kwargs: monitor_openapi.ProbeResult(case.operation_id, 500, 1, False),
    )

    summary = monitor_openapi.run_cases(
        cases,
        base_url="http://monitor.invalid",
        timeout=1,
        workers=1,
    )

    assert summary["failure_count"] == 21
    assert summary["first_failure_operation_id"] == "failed-0"
    assert summary["failure_truncation_count"] == 1
    assert len(summary["failures"]) == 20


def test_execute_case_rejects_documented_non_2xx_status(monkeypatch) -> None:
    class Response:
        status = 404
        headers = {"Content-Type": "application/json"}

        def read(self, _size):
            return b"{}"

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(monitor_openapi, "open_no_redirect", lambda *_args, **_kwargs: Response())

    result = monitor_openapi.execute_case(
        monitor_openapi.ProbeCase("missing", "/missing", (200, 404), 2000),
        base_url="http://monitor.invalid",
        timeout=1,
    )

    assert result.status == 404
    assert result.ok is False


def test_monitor_bounds_generated_list_parameters() -> None:
    spec_by_path = {
        "paths": {
            "/api/v1/drug/list-product/all/{page}/{results_per_page}": {
                "get": {
                    "x-monitor": {"mode": "bounded", "max_latency_ms": 2000},
                    "responses": {"200": {"description": "page"}},
                    "parameters": [
                        {"name": "page", "in": "path", "example": 99},
                        {"name": "results_per_page", "in": "path", "example": 99},
                        {"name": "limit", "in": "query", "example": 99},
                    ],
                }
            }
        }
    }

    case = monitor_openapi.operation_cases(spec_by_path)[0]

    assert case.path.endswith("/0/5?limit=5")


def test_execute_case_never_sends_bearer_or_follows_redirects(monkeypatch) -> None:
    captured_request_by_key = {}

    class Response:
        status = 200
        headers = {"Content-Type": "application/json"}

        def read(self, _size):
            return b"{}"

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    class Opener:
        def open(self, request, timeout):
            captured_request_by_key["request"] = request
            captured_request_by_key["timeout"] = timeout
            return Response()

    def fake_build_opener(handler):
        captured_request_by_key["handler"] = handler
        return Opener()

    monkeypatch.setattr(monitor_openapi.urllib.request, "build_opener", fake_build_opener)

    probe_result = monitor_openapi.execute_case(
        monitor_openapi.ProbeCase("healthy", "/healthy", (200,), 2000),
        base_url="http://monitor.invalid",
        timeout=1,
    )

    assert probe_result.ok is True
    assert isinstance(captured_request_by_key["handler"], monitor_openapi.NoRedirect)
    assert captured_request_by_key["request"].get_header("Authorization") is None


def test_execute_case_rejects_non_json_success(monkeypatch) -> None:
    class Response:
        status = 200
        headers = {"Content-Type": "text/html"}

        def read(self, _size):
            return b"<html>login</html>"

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(
        monitor_openapi,
        "open_no_redirect",
        lambda *_args, **_kwargs: Response(),
    )
    result = monitor_openapi.execute_case(
        monitor_openapi.ProbeCase("healthy", "/healthy", (200,), 2000),
        base_url="http://monitor.invalid",
        timeout=1,
    )

    assert result.ok is False
    assert result.error == "invalid_content_type"


def test_execute_case_rejects_invalid_or_oversized_json_and_off_origin_path(monkeypatch) -> None:
    class Response:
        status = 200
        headers = {"Content-Type": "application/json"}

        def __init__(self, body: bytes):
            self.body = body

        def read(self, _size):
            return self.body

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(
        monitor_openapi,
        "open_no_redirect",
        lambda *_args, **_kwargs: Response(b"not-json"),
    )
    invalid = monitor_openapi.execute_case(
        monitor_openapi.ProbeCase("invalid", "/invalid", (200,), 2000),
        base_url="http://monitor.invalid",
        timeout=1,
    )
    assert invalid.error == "invalid_json"

    assert (
        monitor_openapi.response_error(
            b"x" * (monitor_openapi.MAX_RESPONSE_BYTES + 1), "application/json"
        )
        == "response_too_large"
    )
    with pytest.raises(ValueError, match="configured origin"):
        monitor_openapi.execute_case(
            monitor_openapi.ProbeCase("off-origin", "//elsewhere.invalid", (200,), 2000),
            base_url="https://monitor.invalid",
            timeout=1,
        )


def test_kuma_push_failure_redacts_secret_url(monkeypatch) -> None:
    def fail(*_args, **_kwargs):
        raise monitor_openapi.urllib.error.URLError("opaque-secret-token")

    monkeypatch.setattr(monitor_openapi, "open_no_redirect", fail)
    with pytest.raises(RuntimeError, match="^Kuma push failed$") as exc_info:
        monitor_openapi.push_summary(
            "https://kuma.invalid/api/push/opaque-secret-token",
            {
                "operation_count": 1,
                "failure_count": 1,
                "p95_ms": 1,
                "ok": False,
            "first_failure_operation_id": "missing",
            "failure_truncation_count": 0,
            "failures": [],
            },
        )

    assert "opaque-secret-token" not in str(exc_info.value)


def test_kuma_push_rejects_template_query() -> None:
    with pytest.raises(ValueError, match="bare HTTP"):
        monitor_openapi.push_summary(
            "https://kuma.invalid/api/push/token?status=up&msg=OK&ping=",
            {
                "operation_count": 1,
                "failure_count": 0,
                "p95_ms": 1,
                "ok": True,
                "first_failure_operation_id": None,
                "failure_truncation_count": 0,
                "failures": [],
            },
        )


def test_kuma_push_includes_first_failure_and_truncation_without_redirects(monkeypatch) -> None:
    captured_push_by_key = {}

    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    def open_push(request, timeout):
        captured_push_by_key["request"] = request
        captured_push_by_key["timeout"] = timeout
        return Response()

    monkeypatch.setattr(monitor_openapi, "open_no_redirect", open_push)
    monitor_openapi.push_summary(
        "https://kuma.invalid/api/push/token",
        {
            "operation_count": 25,
            "failure_count": 22,
            "p95_ms": 50,
            "ok": False,
            "first_failure_operation_id": "slow",
            "failure_truncation_count": 2,
            "failures": [
                {
                    "operation_id": "slow",
                    "status": 200,
                    "elapsed_ms": 51,
                    "error": "latency budget exceeded",
                    "max_latency_ms": 50,
                }
            ],
        },
    )

    message = urllib.parse.parse_qs(
        urllib.parse.urlsplit(captured_push_by_key["request"].full_url).query
    )["msg"][0]
    assert "first_failure=slow" in message
    assert "failure_truncated=2" in message
    assert "status=200 error=latency budget exceeded" in message
    assert "elapsed_ms=51 budget_ms=50" in message


@pytest.mark.parametrize(
    ("status", "error"),
    [
        (500, "HTTP 500"),
        (None, "TimeoutError"),
        (200, "invalid_json"),
        (200, "latency budget exceeded"),
    ],
)
def test_kuma_push_distinguishes_sanitized_failure_status_and_error(
    monkeypatch, status, error
) -> None:
    captured_push_by_key = {}

    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    def open_push(request, _timeout):
        captured_push_by_key["request"] = request
        return Response()

    monkeypatch.setattr(monitor_openapi, "open_no_redirect", open_push)
    monitor_openapi.push_summary(
        "https://kuma.invalid/api/push/token",
        {
            "operation_count": 1,
            "failure_count": 1,
            "p95_ms": 101,
            "ok": False,
            "first_failure_operation_id": "probe",
            "failure_truncation_count": 0,
            "failures": [
                {
                    "operation_id": "probe",
                    "status": status,
                    "elapsed_ms": 101,
                    "ok": False,
                    "error": error,
                    "max_latency_ms": 100,
                }
            ],
        },
    )

    message = urllib.parse.parse_qs(
        urllib.parse.urlsplit(captured_push_by_key["request"].full_url).query
    )["msg"][0]
    expected_status = status if status is not None else "-"
    assert f"status={expected_status} error={error}" in message
