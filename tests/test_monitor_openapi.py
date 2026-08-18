"""Contracts for the bounded live Drug API monitor."""

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
    assert all("{" not in case.path for case in cases)


@pytest.mark.parametrize("method", sorted(monitor_openapi.HTTP_METHODS - {"get"}))
def test_monitor_cases_fail_closed_for_non_get_operation(method: str) -> None:
    spec_by_path = {
        "paths": {
            "/unsafe": {
                method: {
                    "operationId": "unsafeMutation",
                    "responses": {"204": {"description": "done"}},
                }
            }
        }
    }

    try:
        monitor_openapi.operation_cases(spec_by_path)
    except ValueError as exc:
        assert "needs an explicit monitoring safety policy" in str(exc)
    else:
        raise AssertionError("non-GET operation was accepted")


def test_run_cases_reports_server_error_and_latency(monkeypatch) -> None:
    cases = [
        monitor_openapi.ProbeCase("healthy", "/healthy", (200,)),
        monitor_openapi.ProbeCase("unready", "/ready", (200, 503)),
    ]

    def fake_execute(case, **_kwargs):
        if case.operation_id == "healthy":
            return monitor_openapi.ProbeResult("healthy", 200, 40, True)
        return monitor_openapi.ProbeResult("unready", 503, 240, False)

    monkeypatch.setattr(monitor_openapi, "execute_case", fake_execute)
    summary = monitor_openapi.run_cases(
        cases,
        base_url="http://monitor.invalid",
        api_key="",
        timeout=1,
        workers=1,
        max_p95_ms=200,
    )

    assert summary["ok"] is False
    assert summary["operation_count"] == 2
    assert summary["failure_count"] == 1
    assert summary["p95_ms"] == 240


def test_execute_case_rejects_documented_non_2xx_status(monkeypatch) -> None:
    class Response:
        status = 404

        def read(self, _size):
            return b""

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(monitor_openapi.urllib.request, "urlopen", lambda *_args, **_kwargs: Response())

    result = monitor_openapi.execute_case(
        monitor_openapi.ProbeCase("missing", "/missing", (200, 404)),
        base_url="http://monitor.invalid",
        api_key="",
        timeout=1,
    )

    assert result.status == 404
    assert result.ok is False


def test_monitor_bounds_generated_list_parameters() -> None:
    spec_by_path = {
        "paths": {
            "/api/v1/drug/list-product/all/{page}/{results_per_page}": {
                "get": {
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


def test_kuma_push_failure_redacts_secret_url(monkeypatch) -> None:
    def fail(*_args, **_kwargs):
        raise monitor_openapi.urllib.error.URLError("opaque-secret-token")

    monkeypatch.setattr(monitor_openapi.urllib.request, "urlopen", fail)
    with pytest.raises(RuntimeError, match="^Kuma push failed$") as exc_info:
        monitor_openapi.push_summary(
            "https://kuma.invalid/api/push/opaque-secret-token",
            {"operation_count": 1, "failure_count": 1, "p95_ms": 1, "ok": False},
        )

    assert "opaque-secret-token" not in str(exc_info.value)


def test_kuma_push_rejects_template_query() -> None:
    with pytest.raises(ValueError, match="query or fragment"):
        monitor_openapi.push_summary(
            "https://kuma.invalid/api/push/token?status=up&msg=OK&ping=",
            {"operation_count": 1, "failure_count": 0, "p95_ms": 1, "ok": True},
        )
