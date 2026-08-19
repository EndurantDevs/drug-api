"""Contracts for the bounded live Drug API monitor."""

import urllib.parse

import pytest

from scripts import monitor_openapi

FAILURE_REASON_CASES = (
    ("HTTP 500", 500, "HTTP500"),
    ("TimeoutError", 200, "TIMEOUT"),
    ("URLError", 200, "NETWORK"),
    ("invalid_content_type", 200, "CONTENT"),
    ("response_too_large", 200, "OVERSIZE"),
    ("invalid_json", 200, "BADJSON"),
    ("latency budget exceeded", 200, "LAT"),
    ("private-marker", 200, "ERROR"),
)


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


def test_monitor_codes_cover_exact_active_inventory() -> None:
    spec = monitor_openapi.load_spec(monitor_openapi.DEFAULT_OPENAPI_PATH)
    active_operation_ids = [
        case.operation_id
        for case in monitor_openapi.operation_cases(spec)
        if case.path is not None
    ]

    assert list(monitor_openapi.MONITOR_OPERATION_CODES) == active_operation_ids
    assert list(monitor_openapi.MONITOR_OPERATION_CODES.values()) == [
        f"DA{index:03d}" for index in range(1, len(active_operation_ids) + 1)
    ]


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


def test_run_cases_retains_every_failure(monkeypatch) -> None:
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
    assert [failure["operation_id"] for failure in summary["failures"]] == [
        f"failed-{index}" for index in range(21)
    ]


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


class _PushResponse:
    status = 200

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


def _capture_push_url(monkeypatch, summary) -> str:
    captured_push_by_key = {}

    def open_push(request, _timeout):
        captured_push_by_key["request"] = request
        return _PushResponse()

    monkeypatch.setattr(monitor_openapi, "open_no_redirect", open_push)
    monitor_openapi.push_summary("https://kuma.invalid/api/push/token", summary)
    return captured_push_by_key["request"].full_url


def _probe_failure(operation_id: str, index: int) -> dict:
    error, status, _reason = FAILURE_REASON_CASES[index % len(FAILURE_REASON_CASES)]
    return {
        "operation_id": operation_id,
        "status": status,
        "elapsed_ms": 2001,
        "ok": False,
        "error": error,
        "max_latency_ms": 2000,
        "path": "/private-marker/live-id",
        "url": "https://private-marker.invalid/?key=private-marker",
        "body": "private-marker",
        "headers": "private-marker",
    }


def test_kuma_push_lists_every_sanitized_failure_within_bounds(monkeypatch) -> None:
    probe_cases = [
        case
        for case in monitor_openapi.operation_cases(
            monitor_openapi.load_spec(monitor_openapi.DEFAULT_OPENAPI_PATH)
        )
        if case.path is not None
    ]
    failures = [
        _probe_failure(case.operation_id, index) for index, case in enumerate(probe_cases)
    ]
    request_url = _capture_push_url(
        monkeypatch,
        {
            "operation_count": 25,
            "failure_count": len(failures),
            "p95_ms": 2001,
            "ok": False,
            "failures": failures,
        },
    )

    message = urllib.parse.parse_qs(urllib.parse.urlsplit(request_url).query)["msg"][0]
    diagnostic_points = message.split(" points=", 1)[1].split(",")
    expected_points = [
        f"DA{index:03d}:{FAILURE_REASON_CASES[(index - 1) % len(FAILURE_REASON_CASES)][2]}:2001/2000"
        for index in range(1, len(probe_cases) + 1)
    ]

    assert diagnostic_points == expected_points
    assert "private-marker" not in message
    assert "live-id" not in message
    assert len(message.encode("ascii")) <= 3000
    assert len(request_url.encode("ascii")) <= 8192


def test_kuma_push_rejects_oversized_encoded_request(monkeypatch) -> None:
    monkeypatch.setattr(
        monitor_openapi,
        "open_no_redirect",
        lambda *_args, **_kwargs: pytest.fail("oversized request was sent"),
    )

    with pytest.raises(RuntimeError, match="^Kuma push failed$"):
        monitor_openapi.push_summary(
            "https://kuma.invalid/api/push/" + "x" * 8192,
            {
                "operation_count": 25,
                "failure_count": 0,
                "p95_ms": 1,
                "ok": True,
                "failures": [],
            },
        )
