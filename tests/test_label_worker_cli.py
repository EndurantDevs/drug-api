"""The existing Label burst command reports job failures and closes its worker."""

import runpy
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock

import arq.cli
import pytest
from arq.jobs import serialize_job
from arq.worker import FailedJobs, create_worker, func
from click.testing import CliRunner
from dotenv import dotenv_values
from sanic import Sanic

from process import Labeling, label_worker_cli


@pytest.fixture
def main_cli(monkeypatch):
    root = Path(__file__).resolve().parents[1]
    for name, setting in dotenv_values(root / ".env.example").items():
        monkeypatch.setenv(name, setting)
    monkeypatch.setattr(label_worker_cli.logging.config, "dictConfig", Mock())
    namespace = runpy.run_path(str(root / "main.py"))
    try:
        yield namespace["cli"]
    finally:
        Sanic.unregister_app(namespace["api"])


@pytest.fixture
def label_cli_worker(monkeypatch):
    state = SimpleNamespace(workers=[], has_publication_failure=False)

    def factory(settings, **options):
        assert settings is Labeling

        async def publish_label(_context):
            if state.has_publication_failure:
                raise RuntimeError("publication failed")

        pipeline = MagicMock()
        pipeline.__aenter__.return_value = pipeline
        pipeline.execute = AsyncMock(
            return_value=[serialize_job("publish_label", (), {}, None, 0, serializer=Labeling.job_serializer), 1, True]
        )
        redis = MagicMock()
        redis.pipeline.return_value = pipeline
        redis.all_job_results = AsyncMock(side_effect=AssertionError("unrelated retained results were scanned"))
        redis.delete = AsyncMock()
        redis.close = AsyncMock()
        worker = create_worker(
            settings,
            functions=[func(publish_label, name="publish_label")],
            on_startup=None,
            on_shutdown=None,
            redis_pool=redis,
            handle_signals=False,
            **options,
        )
        worker.finish_job = AsyncMock()
        worker.async_run = lambda: worker.run_job("synthetic-label-job", 0)
        state.workers.append(SimpleNamespace(worker=worker, redis=redis))
        return worker

    monkeypatch.setattr(label_worker_cli, "create_worker", factory)
    return state


@pytest.mark.parametrize("failed", [False, True])
def test_label_burst_reports_publication_outcome(main_cli, label_cli_worker, failed):
    label_cli_worker.has_publication_failure = failed
    invocation = CliRunner().invoke(main_cli, ["worker", "process.Labeling", "--burst"])
    assert invocation.exit_code == int(failed)
    if failed:
        assert isinstance(invocation.exception, FailedJobs)
    observed = label_cli_worker.workers[0]
    assert observed.worker.jobs_failed == int(failed)
    assert observed.worker.jobs_complete == int(not failed)
    assert observed.worker.keep_result_s == 0
    assert observed.worker.finish_job.call_args.args[2] is None
    observed.redis.all_job_results.assert_not_awaited()
    observed.redis.delete.assert_awaited_once_with(observed.worker.health_check_key)
    observed.redis.close.assert_awaited_once_with(close_connection_pool=True)


def test_label_burst_closes_worker_when_execution_raises(monkeypatch):
    worker = SimpleNamespace(async_run=AsyncMock(side_effect=RuntimeError("startup failed")), close=AsyncMock())
    monkeypatch.setattr(label_worker_cli, "create_worker", Mock(return_value=worker))
    monkeypatch.setattr(label_worker_cli.logging.config, "dictConfig", Mock())
    invocation = CliRunner().invoke(label_worker_cli.worker_cli, ["process.Labeling", "--burst"])
    assert invocation.exit_code == 1
    assert str(invocation.exception) == "startup failed"
    worker.close.assert_awaited_once()


@pytest.mark.parametrize("custom_log_dict", [None, "example.logging"])
@pytest.mark.parametrize("verbose", [False, True])
def test_label_burst_preserves_logging_options(monkeypatch, custom_log_dict, verbose):
    settings = object()
    log_config_dict = {"version": 1}
    imported = Mock(side_effect=lambda name: settings if name == "process.Labeling" else log_config_dict)
    default_config = Mock(return_value=log_config_dict)
    configure = Mock()
    worker = SimpleNamespace(async_run=AsyncMock(), jobs_failed=0, close=AsyncMock())
    factory = Mock(return_value=worker)
    monkeypatch.setattr(label_worker_cli, "import_string", imported)
    monkeypatch.setattr(label_worker_cli, "default_log_config", default_config)
    monkeypatch.setattr(label_worker_cli.logging.config, "dictConfig", configure)
    monkeypatch.setattr(label_worker_cli, "create_worker", factory)
    options = ["process.Labeling", "--burst"]
    if verbose:
        options.append("--verbose")
    if custom_log_dict:
        options.extend(["--custom-log-dict", custom_log_dict])
    invocation = CliRunner().invoke(label_worker_cli.worker_cli, options)
    assert invocation.exit_code == 0, invocation.exception
    assert imported.call_args_list[0].args == ("process.Labeling",)
    if custom_log_dict:
        imported.assert_any_call(custom_log_dict)
        default_config.assert_not_called()
    else:
        default_config.assert_called_once_with(verbose)
    configure.assert_called_once_with(log_config_dict)
    factory.assert_called_once_with(settings, burst=True)
    worker.close.assert_awaited_once()


@pytest.mark.parametrize(
    "arguments,expected",
    [
        (["process.NDC", "--burst"], {"worker_settings": "process.NDC", "burst": True}),
        (["process.Labeling"], {"burst": None}),
        (["process.Labeling", "--no-burst"], {"burst": False}),
        (["process.Labeling", "--burst", "--check"], {"burst": True, "check": True}),
        (["process.Labeling", "--burst", "--watch", "."], {"burst": True, "watch": "."}),
        (
            ["process.Labeling", "-v", "--custom-log-dict", "example.logging"],
            {"verbose": True, "custom_log_dict": "example.logging"},
        ),
    ],
)
def test_other_worker_modes_delegate_unchanged(monkeypatch, arguments, expected):
    delegate = Mock()
    monkeypatch.setattr(arq.cli.cli, "callback", delegate)
    factory = Mock()
    monkeypatch.setattr(label_worker_cli, "create_worker", factory)
    invocation = CliRunner().invoke(label_worker_cli.worker_cli, arguments)
    assert invocation.exit_code == 0, invocation.exception
    options_by_name = {
        "worker_settings": "process.Labeling",
        "burst": None,
        "check": False,
        "watch": None,
        "verbose": False,
        "custom_log_dict": None,
    }
    delegate.assert_called_once_with(**(options_by_name | expected))
    factory.assert_not_called()


def test_cli_reuses_stock_parameters_and_version(monkeypatch):
    assert label_worker_cli.worker_cli.params == arq.cli.cli.params
    factory = Mock()
    monkeypatch.setattr(label_worker_cli, "create_worker", factory)
    actual = CliRunner().invoke(label_worker_cli.worker_cli, ["--version"])
    stock = CliRunner().invoke(arq.cli.cli, ["--version"])
    assert actual.exit_code == stock.exit_code == 0
    assert actual.output == stock.output
    factory.assert_not_called()


def test_main_worker_entrypoint_loads_stock_options(main_cli):
    invocation = CliRunner().invoke(main_cli, ["worker", "--help"])
    assert invocation.exit_code == 0, invocation.exception
    for option in ("--burst", "--check", "--watch", "--verbose", "--custom-log-dict", "--version"):
        assert option in invocation.output


def test_delegated_worker_preserves_exit_status(monkeypatch):
    monkeypatch.setattr(arq.cli.cli, "callback", Mock(side_effect=SystemExit(7)))
    invocation = CliRunner().invoke(label_worker_cli.worker_cli, ["process.Labeling", "--check"])
    assert invocation.exit_code == 7
