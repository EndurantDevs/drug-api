"""Run the actual Label producer with one synthetic source record in an existing test run."""

import argparse
import asyncio
import importlib
import json
import os
import re
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import event

from db.connection import Database
from process import label_stage


async def produce_stage(run_id, role):
    """Use supplied database settings; the caller owns the run and resource cleanup."""
    if os.getenv("HLTHPRT_ENVIRONMENT") != "test" or not re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", role):
        raise ValueError("requires test environment and a valid role")
    database = Database()
    await database.connect()

    @event.listens_for(database.engine.sync_engine, "connect")
    def set_role(connection, _record):
        cursor = connection.cursor()
        cursor.execute(f'SET ROLE "{role}"')
        cursor.close()

    manifest_dict = {
        "results": {
            "drug": {
                "label": {
                    "total_records": 1,
                    "partitions": [{"file": "https://example.test/part.zip", "records": 1}],
                }
            }
        }
    }

    async def source(ctx, _task, _spec):
        await ctx["redis"].enqueue_job("process_label_results", {"results": [{"id": "synthetic-native-label"}]})

    try:
        with pytest.MonkeyPatch.context() as patch:
            patch.setenv("HLTHPRT_MAIN_RX_JSON_URL", "https://example.test/manifest")
            patch.setattr(
                label_stage,
                "download_it",
                AsyncMock(
                    return_value=SimpleNamespace(content=json.dumps(manifest_dict), raise_for_status=lambda: None)
                ),
            )
            patch.setattr(label_stage, "download_partition_content", source)
            patch.setattr(label_stage, "ensure_import_run_table", AsyncMock())
            patch.setattr(label_stage, "enqueue_status_event", lambda _event: None)
            label = importlib.import_module("process.label")
            return await label_stage.protected_label_import(
                {"context": {}},
                {"run_id": run_id},
                database,
                label._label_row_dict_from_record,
                label.LABEL_DOWNLOAD_SPEC,
            )
    finally:
        await database.disconnect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--role", required=True)
    args = parser.parse_args()
    print(json.dumps(asyncio.run(produce_stage(args.run_id, args.role)), sort_keys=True))
