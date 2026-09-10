"""Partition download metadata stays bound to its control run."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import partition_download
from process.partition_download import PartitionDownloadSpec, _new_batch_task, _partition_context


def test_partition_context_and_batch_keep_control_identity():
    context = _partition_context(
        {"context": {"control_run_id": "run-1"}},
        {"what": "label", "max_records": "7", "partition_index": "2", "partition_count": "3"},
    )
    spec = PartitionDownloadSpec("label", "DrugLabel", "save-labels", "download", "labels")

    assert context.run_id == "run-1"
    assert (context.max_records, context.partition_records) == (7, 7)
    assert (context.partition_index, context.partition_count) == (2, 3)
    assert _new_batch_task({"what": "label"}, context, spec) == {
        "what": "label",
        "model": "DrugLabel",
        "results": [],
        "run_id": "run-1",
        "partition_records": 7,
    }


@pytest.mark.asyncio
async def test_partition_batch_emits_exact_progress(monkeypatch):
    events = []
    monkeypatch.setattr(partition_download, "enqueue_live_progress", lambda **event: events.append(event))
    redis = SimpleNamespace(enqueue_job=AsyncMock())
    context = partition_download._PartitionContext("run-2", 0, 12, 2, 3)
    spec = PartitionDownloadSpec("label", "DrugLabel", "save-labels", "download", "labels")
    batch_dict = {"results": [{"id": 1}]}

    await partition_download._enqueue_batch(redis, batch_dict, 4, context, spec)
    partition_download._enqueue_partition_started(context, spec)
    partition_download._enqueue_partition_parsed(12, context, spec)

    redis.enqueue_job.assert_awaited_once_with("save-labels", {"results": [{"id": 1}], "batch_end": 4})
    assert [(event["phase"], event["done"], event["total"]) for event in events] == [
        ("label parsing records", 4, 12),
        ("label downloading partition", 1, 3),
        ("label partition parsed", 12, 12),
    ]


@pytest.mark.asyncio
async def test_partition_archive_is_extracted_before_batching(monkeypatch):
    calls = []

    async def download(url, destination):
        calls.append(("download", url, destination))

    async def extract(archive, destination):
        calls.append(("extract", archive, destination))

    async def enqueue(redis, task, context, spec, json_file):
        calls.append(("enqueue", json_file))
        return 2

    monkeypatch.setattr(partition_download, "download_it_and_save", download)
    monkeypatch.setattr(partition_download, "unzip", extract)
    monkeypatch.setattr(partition_download, "_enqueue_batches_from_json", enqueue)
    context = partition_download._PartitionContext("run-3", 0, 0, 1, 1)
    spec = PartitionDownloadSpec("label", "DrugLabel", "save-labels", "download", "labels")

    assert await partition_download._download_and_enqueue_batches(
        object(), {"file": "https://example.test/partition.json.zip"}, context, spec
    ) == 2
    assert [call[0] for call in calls] == ["download", "extract", "enqueue"]
    assert calls[0][2].endswith("/partition.json.zip")
    assert calls[1][1] == calls[0][2]
    assert calls[2][1].endswith("/partition.json")
