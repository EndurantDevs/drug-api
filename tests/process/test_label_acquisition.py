"""Label acquisition remains bounded and publishes only a complete private stage."""

import asyncio
import copy
import hashlib
import importlib
import json
import os
import shutil
import subprocess
import sys
import zipfile
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import msgpack
import pytest
from arq.jobs import serialize_job
from arq.worker import create_worker

from api import control_imports, control_run_store
from process import Labeling, control_lifecycle, partition_download

label = importlib.import_module("process.label")


@asynccontextmanager
async def _storage_transaction(database, stage_rows_by_name):
    before_by_name = copy.deepcopy(stage_rows_by_name)

    async def create_bound_table(create, **options):
        await database.create_table(create.__self__, **options)

    connection = SimpleNamespace(
        dialect=SimpleNamespace(identifier_preparer=SimpleNamespace(quote_schema=lambda name: f'"{name}"')),
        exec_driver_sql=AsyncMock(),
        run_sync=AsyncMock(side_effect=create_bound_table),
    )
    try:
        yield SimpleNamespace(connection=AsyncMock(return_value=connection))
    except BaseException:
        stage_rows_by_name.clear()
        stage_rows_by_name.update(before_by_name)
        raise


@pytest.fixture
def label_storage(monkeypatch):
    """Replace database I/O; native publication tests cover the transaction boundary."""
    stage_rows_by_name = {"label": [{"id": "current"}], "label_20261111": [{"id": "retained"}]}
    calls = []

    async def create_table(table, *, checkfirst):
        assert checkfirst is False
        if table.name in stage_rows_by_name:
            raise RuntimeError("stage already exists")
        stage_rows_by_name[table.name] = []

    async def save(rows, model):
        stage_rows_by_name[model.__tablename__].extend(rows)
        calls.append(("save", [row["id"] for row in rows]))

    async def complete(_database, context):
        suffix = context["import_date"]
        count = len(stage_rows_by_name["label_" + suffix])
        if count != context["context"]["label_count"]:
            raise ValueError("saved census differs")
        calls.append(("publish", suffix))
        stage_rows_by_name["label"] = list(stage_rows_by_name["label_" + suffix])
        return {
            "format": "label-publication-v1",
            "run_id": context["control_run_id"],
            "source_label_count": count,
            "imported_label_count": count,
            "published": True,
        }

    database = SimpleNamespace(
        create_table=AsyncMock(side_effect=create_table),
        disconnect=AsyncMock(),
        scalar=AsyncMock(side_effect=lambda statement, **_params: 42 if "to_regclass" in str(statement) else True),
    )
    database.transaction = lambda: _storage_transaction(database, stage_rows_by_name)
    monkeypatch.setattr(label, "db", database)
    monkeypatch.setattr(label, "push_objects", AsyncMock(side_effect=save))
    monkeypatch.setattr(label, "complete_label_publication", AsyncMock(side_effect=complete))
    return SimpleNamespace(
        stage_rows_by_name=stage_rows_by_name,
        original=copy.deepcopy(stage_rows_by_name),
        database=database,
        calls=calls,
    )


@pytest.fixture
def acquisition(monkeypatch, label_storage):
    """Exercise real normalization and control flow with synthetic source and storage."""
    manifest_dict = {
        "total_records": 3,
        "partitions": [
            {"file": "https://example.test/one.json.zip", "records": 2},
            {"file": "https://example.test/two.json.zip", "records": 1},
        ],
    }

    async def download(context, task, _spec):
        label_storage.calls.append(("download", task["file"]))
        records = [{"id": task["file"] + str(index)} for index in range(task["partition_records"])]
        await context["redis"].enqueue_job(
            "process_label_results",
            {
                "results": records,
                "run_id": task["run_id"],
                "batch_end": len(records),
                "partition_records": task["partition_records"],
            },
        )

    response = SimpleNamespace(raise_for_status=Mock())

    async def read_manifest(_url):
        response.content = json.dumps({"results": {"drug": {"label": manifest_dict}}}).encode()
        return response

    monkeypatch.setenv("HLTHPRT_MAIN_RX_JSON_URL", "https://example.test/download.json")
    monkeypatch.setenv("HLTHPRT_IMPORT_LIVE_PROGRESS_HEARTBEAT_SECONDS", "0")
    monkeypatch.setattr(label, "download_it", AsyncMock(side_effect=read_manifest))
    monkeypatch.setattr(label, "download_partition_content", AsyncMock(side_effect=download))
    monkeypatch.setattr(label, "require_label_ordinary_publication", AsyncMock())
    monkeypatch.setattr(label, "print_time_info", Mock())
    monkeypatch.setattr(label, "enqueue_live_progress", Mock())
    monkeypatch.setattr(label, "enqueue_status_event", Mock())
    transition = AsyncMock()
    monkeypatch.setattr(label, "mark_control_run", transition)
    monkeypatch.setattr(control_lifecycle, "mark_control_run", transition)
    monkeypatch.setattr(label, "_record_control_run_transition", AsyncMock(return_value=None))
    claim = AsyncMock()
    monkeypatch.setattr(label, "claim_label_run", claim)
    monkeypatch.setattr(control_lifecycle, "claim_label_run", claim)
    monkeypatch.setattr(control_lifecycle, "_flush_terminal_status_events", AsyncMock())
    label_storage.redis = SimpleNamespace(
        enqueue_job=AsyncMock(side_effect=AssertionError("record batch reached Redis"))
    )
    label_storage.manifest_dict, label_storage.response, label_storage.transition = manifest_dict, response, transition
    return label_storage


@pytest.mark.asyncio
async def test_normal_api_payload_runs_all_partitions_before_managed_success(acquisition, monkeypatch):
    queued = SimpleNamespace(job_id="example-job")
    producer = SimpleNamespace(enqueue_job=AsyncMock(return_value=queued))
    monkeypatch.setattr(control_imports, "create_pool", AsyncMock(return_value=producer))
    run_record_dict = {"run_id": "example-run", "importer": "label", "family": "drug", "params": {}}
    enqueued = await control_imports._enqueue(control_imports._IMPORTERS["label"], run_record_dict)
    assert enqueued["status"] == "queued"
    function, payload = producer.enqueue_job.call_args.args
    assert function == "control_single_job_start"
    assert payload["target_module"] == "process.label" and payload["target_function"] == "init_label_file"
    import_context_dict = {"redis": acquisition.redis, "job_id": queued.job_id}

    result = await control_lifecycle.control_single_job_start(import_context_dict, payload)

    assert result["status"] == "succeeded"
    assert result["result"] == {
        "format": "label-publication-v1",
        "run_id": run_record_dict["run_id"],
        "source_label_count": 3,
        "imported_label_count": 3,
        "published": True,
    }
    assert [call[0] for call in acquisition.calls] == ["download", "save", "download", "save", "publish"]
    assert len(acquisition.stage_rows_by_name["label"]) == 3
    assert acquisition.stage_rows_by_name["label_20261111"] == acquisition.original["label_20261111"]
    assert "import_date" not in import_context_dict and "context" not in import_context_dict
    acquisition.redis.enqueue_job.assert_not_awaited()
    acquisition.response.raise_for_status.assert_called_once()


@pytest.mark.asyncio
async def test_repeated_or_foreign_stage_is_refused_without_reset(acquisition):
    run_id = "example-run"
    name = "label_l" + hashlib.sha256(run_id.encode()).hexdigest()[:32]
    acquisition.stage_rows_by_name[name] = [{"id": "foreign"}]
    before = copy.deepcopy(acquisition.stage_rows_by_name)

    with pytest.raises(RuntimeError, match="stage already exists"):
        await label.init_label_file({"redis": acquisition.redis}, {"run_id": run_id})

    assert acquisition.stage_rows_by_name == before
    label.download_partition_content.assert_not_awaited()
    assert all(event[0] != "publish" for event in acquisition.calls)


@pytest.mark.asyncio
async def test_sequential_imports_have_distinct_stages_and_independent_counts(acquisition):
    for run_id in ("first-run", "second-run"):
        result = await label.init_label_file({"redis": acquisition.redis}, {"run_id": run_id})
        assert result["source_label_count"] == result["imported_label_count"] == 3
    created_names = [call.args[0].name for call in acquisition.database.create_table.call_args_list]
    assert len(set(created_names)) == 2
    assert all(len(acquisition.stage_rows_by_name[name]) == 3 for name in created_names)
    assert acquisition.stage_rows_by_name["label_20261111"] == acquisition.original["label_20261111"]


@pytest.mark.asyncio
async def test_standalone_and_capped_sample_entry_points_remain_available(acquisition):
    result = await label.init_label_file({"redis": acquisition.redis, "job_id": "standalone"})
    assert result["run_id"] is None and result["imported_label_count"] == 3
    result = await label.init_label_file(
        {"redis": acquisition.redis}, {"run_id": "sample-run", "test_mode": True, "max_records": 1}
    )
    assert result["source_label_count"] == result["imported_label_count"] == 1


@pytest.mark.asyncio
async def test_real_streamed_archive_matches_queued_projection_with_no_redis_record_batches(
    acquisition, monkeypatch, tmp_path
):
    label_records = [
        {
            "id": f"record-{index}",
            "indications_and_usage": ["first", "second"],
            "openfda": {"product_ndc": [f"00000-{index:04}"], "package_ndc": [f"00000-{index:04}-01"]},
        }
        for index in range(205)
    ]
    archive = tmp_path / "one.json.zip"
    with zipfile.ZipFile(archive, "w") as output:
        output.writestr("one.json", json.dumps({"results": label_records}))

    async def copy_archive(_url, destination):
        shutil.copyfile(archive, destination)

    monkeypatch.setenv("SAVE_PER_PACK", "100")
    monkeypatch.setattr(partition_download, "download_it_and_save", copy_archive)
    monkeypatch.setattr(partition_download, "enqueue_live_progress", Mock())
    monkeypatch.setattr(label, "download_partition_content", partition_download.download_partition_content)
    acquisition.manifest_dict.update(
        total_records=len(label_records),
        partitions=[{"file": "https://example.test/one.json.zip", "records": len(label_records)}],
    )
    task_dict = {
        "file": acquisition.manifest_dict["partitions"][0]["file"],
        "partition_records": len(label_records),
        "run_id": "stream-run",
    }
    queued_batches = []

    async def collect(_function, batch):
        queued_batches.append(copy.deepcopy(batch))

    await partition_download.download_partition_content(
        {"redis": SimpleNamespace(enqueue_job=collect)}, task_dict, label.LABEL_DOWNLOAD_SPEC
    )
    publication_receipt = await label.init_label_file({"redis": acquisition.redis}, {"run_id": task_dict["run_id"]})

    columns = list(label.Label.__table__.columns.keys())
    expected_rows = [
        label._label_row_dict_from_record(label_record, columns)
        for batch in queued_batches
        for label_record in batch["results"]
    ]
    assert publication_receipt["imported_label_count"] == len(expected_rows) == len(label_records)
    assert acquisition.stage_rows_by_name["label"] == expected_rows
    assert len(queued_batches) == 3
    acquisition.redis.enqueue_job.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [False, True])
async def test_partition_failure_or_cancellation_retains_stage_and_current_generation(acquisition, cancel):
    async def fail_after_save(context, task, _spec):
        await context["redis"].enqueue_job(
            "process_label_results",
            {
                "results": [{"id": "partial"}],
                "run_id": task["run_id"],
            },
        )
        raise asyncio.CancelledError() if cancel else RuntimeError("partition failed")

    label.download_partition_content.side_effect = fail_after_save
    with pytest.raises(asyncio.CancelledError if cancel else RuntimeError):
        await label.init_label_file({"redis": acquisition.redis}, {"run_id": "failed-run"})
    await label.label_shutdown({})

    assert acquisition.stage_rows_by_name["label"] == acquisition.original["label"]
    assert acquisition.stage_rows_by_name["label_20261111"] == acquisition.original["label_20261111"]
    retained_stages = [rows for name, rows in acquisition.stage_rows_by_name.items() if name.startswith("label_l")]
    assert len(retained_stages) == 1
    assert [row["id"] for row in retained_stages[0]] == ["partial"]
    assert all(event[0] != "publish" for event in acquisition.calls)
    assert acquisition.transition.call_args.kwargs["status"] == ("canceled" if cancel else "failed")
    acquisition.database.disconnect.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["manifest", "partition", "saved"])
async def test_incomplete_source_or_saved_census_cannot_publish(acquisition, damage):
    if damage == "manifest":
        acquisition.manifest_dict["total_records"] += 1
    elif damage == "partition":
        original = label.download_partition_content.side_effect

        async def missing_record(context, task, spec):
            await original(context, {**task, "partition_records": 1}, spec)

        label.download_partition_content.side_effect = missing_record
    else:
        label.push_objects.side_effect = None
    with pytest.raises(ValueError):
        await label.init_label_file({"redis": acquisition.redis}, {"run_id": "incomplete-run"})
    assert all(event[0] != "publish" for event in acquisition.calls)
    assert acquisition.stage_rows_by_name["label"] == acquisition.original["label"]


@pytest.mark.asyncio
async def test_unrecorded_managed_intent_stops_before_partition_acquisition(acquisition):
    acquisition.database.scalar.side_effect = lambda statement, **_params: (
        42 if "to_regclass" in str(statement) else False
    )
    with pytest.raises(RuntimeError, match="retained attempt was not recorded"):
        await label.init_label_file({"redis": acquisition.redis}, {"run_id": "unrecorded-run"})
    label.download_partition_content.assert_not_awaited()
    assert acquisition.stage_rows_by_name["label"] == acquisition.original["label"]


@pytest.mark.asyncio
@pytest.mark.parametrize("deferred", [False, True])
async def test_transition_recording_defers_projection(monkeypatch, deferred):
    writes = SimpleNamespace(status=AsyncMock(return_value=1))
    setup = AsyncMock()
    status_event, live_event = Mock(), Mock()
    monkeypatch.setattr(control_lifecycle, "db", writes)
    monkeypatch.setattr(control_lifecycle, "ensure_import_run_table", setup)
    monkeypatch.setattr(control_lifecycle, "enqueue_status_event", status_event)
    monkeypatch.setattr(control_lifecycle, "enqueue_live_progress", live_event)
    function = control_lifecycle._record_control_run_transition if deferred else control_lifecycle.mark_control_run
    projection = await function("example", status="running", phase_detail="selected", progress_message="selected")
    if deferred:
        assert projection[0].run_id == "example" and projection[1]["message"] == "selected"
        setup.assert_not_awaited()
        status_event.assert_not_called()
        live_event.assert_not_called()
    else:
        assert projection is None
        setup.assert_awaited_once()
        assert status_event.call_args.args[0]["run_id"] == "example"
        live_event.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("projection_error", [False, True])
async def test_stage_projects_only_after_commit(acquisition, monkeypatch, projection_error):
    phases = []
    original = acquisition.database.transaction

    @asynccontextmanager
    async def transaction():
        async with original() as session:
            yield session
            assert phases == []
        phases.append("committed")

    def emit(transition, progress):
        assert phases == ["committed"]
        assert transition.metrics["label_publication_intent"]["stage_oid"] == 42
        assert progress["message"] == "selected 2 partition(s)"
        if projection_error:
            raise RuntimeError("projection unavailable")
        phases.append("projected")

    acquisition.database.status = AsyncMock(return_value=1)
    acquisition.database.transaction = transaction
    monkeypatch.setattr(control_lifecycle, "db", acquisition.database)
    monkeypatch.setattr(label, "_record_control_run_transition", control_lifecycle._record_control_run_transition)
    monkeypatch.setattr(label, "_emit_control_run_transition", Mock(side_effect=emit))
    result = await label.init_label_file({"redis": acquisition.redis}, {"run_id": "example"})
    assert result["published"] is True and result["imported_label_count"] == 3
    assert phases == (["committed"] if projection_error else ["committed", "projected"])


@pytest.mark.asyncio
async def test_inline_batch_backpressure_waits_for_saving_before_producer_continues(acquisition):
    started, release = asyncio.Event(), asyncio.Event()
    original = label.push_objects.side_effect

    async def blocked_save(rows, model):
        started.set()
        await release.wait()
        await original(rows, model)

    label.push_objects.side_effect = blocked_save
    worker = asyncio.create_task(label.init_label_file({"redis": acquisition.redis}, {"run_id": "bounded-run"}))
    try:
        await asyncio.wait_for(started.wait(), 5)
        assert acquisition.calls == [("download", acquisition.manifest_dict["partitions"][0]["file"])]
        assert not worker.done()
        acquisition.redis.enqueue_job.assert_not_awaited()
        release.set()
        await worker
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)


@pytest.mark.asyncio
async def test_uncoordinated_or_foreign_legacy_batches_refuse_before_download_or_write(acquisition):
    for context in ({}, {"import_date": "example", "context": {}, "control_run_id": "other-run"}):
        task_dict = {"run_id": "example-run", "results": [], "partition_records": 1}
        with pytest.raises(RuntimeError):
            await label.download_label_content(context, task_dict)
        with pytest.raises(RuntimeError):
            await label.process_label_results(context, task_dict)
    label.download_partition_content.assert_not_awaited()
    label.push_objects.assert_not_awaited()


@pytest.mark.asyncio
async def test_label_worker_is_serial_nonretrying_and_does_not_retain_record_results():
    worker = create_worker(Labeling, redis_pool=Mock(), handle_signals=False)
    assert worker.max_jobs == 1 and worker.retry_jobs is False and worker.keep_result_s == 0
    assert worker.job_timeout_s == 86400
    raw = serialize_job(
        "process_label_results",
        ({"run_id": "example-run", "results": [{"id": "retained"}]},),
        {},
        None,
        0,
        serializer=Labeling.job_serializer,
    )
    assert Labeling.job_deserializer(raw)["a"][0]["results"] == [{"id": "retained"}]
    assert Labeling.job_serializer is msgpack.packb


@pytest.mark.parametrize("damage", ["run", "format", "unpublished", "module"])
def test_managed_success_requires_the_exact_label_publication_receipt(damage):
    task = control_lifecycle.ControlTask("example-run", "label", "process.label", "init_label_file", "ctx_task", {})
    publication_receipt_dict = {"format": "label-publication-v1", "run_id": task.run_id, "published": True}
    if damage == "run":
        publication_receipt_dict["run_id"] = "other-run"
    elif damage == "format":
        publication_receipt_dict["format"] = "foreign-format"
    elif damage == "unpublished":
        publication_receipt_dict["published"] = False
    else:
        task = control_lifecycle.ControlTask(
            task.run_id, task.importer, "process.other", task.target_function, task.call_style, task.target_task_dict
        )
    assert control_lifecycle._native_result_status(task, publication_receipt_dict) == "running"


@pytest.mark.parametrize("timeout", ["0", "86401", "invalid"])
def test_label_timeout_rejects_unbounded_or_invalid_configuration(timeout):
    result = subprocess.run(
        [sys.executable, "-c", "import process"],
        env={**os.environ, "HLTHPRT_LABEL_JOB_TIMEOUT_SECONDS": timeout},
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    expected = (
        "invalid literal for int()"
        if timeout == "invalid"
        else "Label job timeout must be between one second and one day"
    )
    assert expected in result.stderr


def test_existing_queue_setting_selects_api_producer_registry_and_worker_together():
    script = """
import importlib
from api import control_imports, control_workers
from process import Labeling
label = importlib.import_module('process.label')
expected = 'arq:queue:label-example'
assert control_imports._IMPORTERS['label']['queue'] == expected
assert control_workers._BY_IMPORTER['label'].queue == expected
assert Labeling.queue_name == expected == label.LABEL_QUEUE_NAME
"""
    subprocess.run(
        [sys.executable, "-c", script],
        check=True,
        cwd=Path(__file__).resolve().parents[2],
        env={**os.environ, "HLTHPRT_ARQ_QUEUE_LABEL": "arq:queue:label-example"},
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["success", "projection-error", "recovery-error"])
async def test_operator_reconciliation_closes_pool_and_preserves_database_outcome(monkeypatch, outcome):
    database = SimpleNamespace(disconnect=AsyncMock())
    reconciled_run_dict = {"run_id": "example-run", "status": "failed", "publication": "unpublished"}
    recover = AsyncMock(
        return_value=reconciled_run_dict,
        side_effect=RuntimeError("recovery refused") if outcome == "recovery-error" else None,
    )
    monkeypatch.setattr(label, "db", database)
    monkeypatch.setattr(label, "init_db", AsyncMock())
    monkeypatch.setattr(label, "reconcile_managed_label_run", recover)
    monkeypatch.setattr(label, "enqueue_status_event", Mock())
    monkeypatch.setattr(
        label,
        "_flush_terminal_status_events",
        AsyncMock(side_effect=RuntimeError("projection unavailable") if outcome == "projection-error" else None),
    )
    if outcome == "recovery-error":
        with pytest.raises(RuntimeError, match="recovery refused"):
            await label.reconcile_label_run("example-run")
    else:
        assert await label.reconcile_label_run("example-run") == reconciled_run_dict
    recover.assert_awaited_once_with(database, "example-run")
    database.disconnect.assert_awaited_once()


def test_operator_reconciliation_option_uses_existing_label_command(monkeypatch):
    from click.testing import CliRunner

    import process

    recover = AsyncMock()
    acquire = AsyncMock(side_effect=AssertionError("recovery must not acquire data"))
    monkeypatch.setattr(process, "reconcile_label_run", recover)
    monkeypatch.setattr(process, "initiate_label_import", acquire)
    result = CliRunner().invoke(process.process_group, ["label", "--reconcile-run", "example-run"])
    assert result.exit_code == 0
    recover.assert_awaited_once_with("example-run")
    acquire.assert_not_awaited()


@pytest.mark.parametrize("run_id", ["", " ", "\t\n"])
def test_blank_reconciliation_never_enqueues_import(monkeypatch, run_id):
    from click.testing import CliRunner

    import process

    recover = AsyncMock()
    acquire = AsyncMock()
    monkeypatch.setattr(process, "reconcile_label_run", recover)
    monkeypatch.setattr(process, "initiate_label_import", acquire)
    result = CliRunner().invoke(process.process_group, ["label", "--reconcile-run", run_id])
    assert result.exit_code == 2
    assert "nonblank managed run ID" in result.output
    recover.assert_not_awaited()
    acquire.assert_not_awaited()


@pytest.mark.asyncio
async def test_label_enqueue_cas_preserves_a_run_that_already_completed(monkeypatch):
    @asynccontextmanager
    async def session():
        yield SimpleNamespace(commit=AsyncMock())

    current_run_dict = {"run_id": "example-run", "importer": "label", "status": "succeeded"}
    writes = SimpleNamespace(status=AsyncMock(return_value=0))
    monkeypatch.setattr(control_run_store, "db", writes)
    monkeypatch.setattr(control_imports, "db", SimpleNamespace(session=session))
    monkeypatch.setattr(control_imports, "ensure_import_run_table", AsyncMock())
    monkeypatch.setattr(control_imports, "insert_import_run", AsyncMock())
    monkeypatch.setattr(control_imports, "_enqueue", AsyncMock(return_value={"status": "queued"}))
    monkeypatch.setattr(control_imports, "get_import_run", AsyncMock(return_value=current_run_dict))
    monkeypatch.setattr(control_imports, "enqueue_status_event", Mock(side_effect=AssertionError("stale event")))
    monkeypatch.setattr(control_imports, "_write_run_live_progress", Mock(side_effect=AssertionError("stale progress")))

    assert await control_imports.create_import_run({"run_id": "example-run", "importer": "label"}) == (
        current_run_dict,
        True,
    )
    statement = str(writes.status.call_args.args[0])
    assert "AND (importer <> 'label' OR status='queued')" in statement
    assert "NOT (COALESCE(metrics, '{}'::jsonb) ? 'ndc_attempt_id')" in statement


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["running", "succeeded", "failed", "canceled"])
async def test_stale_label_queue_cancel_preserves_the_current_state(monkeypatch, status):
    queued_run_dict = {"run_id": "example-run", "importer": "label", "status": "queued", "metrics": {}}
    current_run_dict = {**queued_run_dict, "status": status}
    writes = SimpleNamespace(status=AsyncMock(return_value=0))
    monkeypatch.setattr(control_imports, "db", writes)
    monkeypatch.setattr(control_imports, "get_import_run", AsyncMock(side_effect=[queued_run_dict, current_run_dict]))
    monkeypatch.setattr(control_imports, "_remove_queued_job", AsyncMock(return_value={"removed": False}))
    monkeypatch.setattr(control_imports, "enqueue_status_event", Mock(side_effect=AssertionError("stale event")))
    monkeypatch.setattr(control_imports, "_write_run_live_progress", Mock(side_effect=AssertionError("stale progress")))

    assert await control_imports.request_cancel(queued_run_dict["run_id"]) is current_run_dict
    assert "AND (importer <> 'label' OR status='queued')" in str(writes.status.call_args.args[0])


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["running", "succeeded", "failed"])
async def test_late_label_transition_keeps_terminal_state_and_suppresses_stale_events(monkeypatch, status):
    writes = SimpleNamespace(status=AsyncMock(return_value=0))
    monkeypatch.setattr(control_lifecycle, "db", writes)
    monkeypatch.setattr(control_lifecycle, "ensure_import_run_table", AsyncMock())
    monkeypatch.setattr(control_lifecycle, "enqueue_status_event", Mock(side_effect=AssertionError("stale event")))
    monkeypatch.setattr(control_lifecycle, "enqueue_live_progress", Mock(side_effect=AssertionError("stale progress")))

    await control_lifecycle.mark_control_run("example-run", status=status, phase_detail=status, progress_message=status)

    statement = str(writes.status.call_args.args[0])
    assert "OR (status='canceling' AND :status='canceled')" in statement
    assert "AND (importer <> 'ndc' OR NOT (COALESCE(metrics, '{}'::jsonb) ? 'ndc_attempt_id'))" in statement
