"""DDL helpers for publishing staged FDA NDC import tables."""

from typing import Any

from db.drug_snapshot_runtime.model_indexes import index_name
from db.drug_snapshot_runtime.ndc import Package, Product
from process.ndc_stage import (
    _lock_ndc_publication,
    audit_ndc_tables,
    check_ndc_incumbents,
    complete_ndc_stages,
    finish_ndc_publication,
    handoff_ndc_stages,
    lock_ndc_stages,
    ndc_publication_mode,
)


async def publish_ndc_tables(database: Any, db_schema: str, import_date: str, *, attempt=None, acquisition=None):
    """Complete owned stages, then hand off or publish under explicit process authority."""
    if attempt is None and ndc_publication_mode() != "direct":
        raise ValueError("NDC handoff requires an owned coordinator attempt")
    if attempt is not None and (attempt.schema != db_schema or attempt.suffix != import_date):
        raise ValueError("NDC publication scope differs from stage owner")
    await database.status("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
    await database.status("CREATE EXTENSION IF NOT EXISTS btree_gin;")
    if attempt is not None:
        counts_before_dict = dict(attempt.counts)
        try:
            async with database.transaction() as session:
                await lock_ndc_stages(database, attempt, exclusive=True)
                await complete_ndc_stages(database, session, attempt)
                receipt = await audit_ndc_tables(database, session, attempt, acquisition)
                if not acquisition["complete"]:
                    return await finish_ndc_publication(database, attempt, receipt, published=False)
                if attempt.publication_mode == "handoff":
                    return await handoff_ndc_stages(database, attempt, receipt)
                await _rename_model_stage_indexes(database, attempt)
                await check_ndc_incumbents(database, attempt)
                for table in ("product", "package"):
                    await _publish_single_ndc_table(database, db_schema, table, import_date)
                return await finish_ndc_publication(database, attempt, receipt, published=True)
        except BaseException:
            attempt.counts.update(counts_before_dict)
            raise
    async with database.transaction():
        for table in ["product", "package"]:
            print(f"Creating indexes for {table} ...")
            model = Product if table == "product" else Package
            index = model.__my_additional_indexes__[0]
            await database.status(
                f"CREATE INDEX {index['staging_name']}_{import_date} ON "
                f"{db_schema}.{table}_{import_date} USING {index['using'].upper()}({', '.join(index['index_elements'])});"
            )
            if table == "product":
                await _create_product_indexes(database, db_schema, import_date)
        await _lock_ndc_publication(database, db_schema)
        for table in ["product", "package"]:
            await _publish_single_ndc_table(database, db_schema, table, import_date)
    return None


async def _rename_model_stage_indexes(database, attempt) -> None:
    """Retain the established direct-mode index names without rebuilding live indexes."""
    for model in (Product, Package):
        table = attempt.tables[model.__tablename__]
        for index in model.__my_additional_indexes__:
            current = index_name(model.__tablename__, f"{table.name}_{model.__tablename__}_idx_{index['name']}")
            staged = f"{index['staging_name']}_{attempt.suffix}"
            await database.status(f'ALTER INDEX "{attempt.schema}"."{current}" RENAME TO "{staged}"')


async def _publish_single_ndc_table(database: Any, db_schema: str, table: str, import_date: str) -> None:
    await database.status(f"DROP TABLE IF EXISTS {db_schema}.{table}_old;")

    await database.status(
        f"ALTER INDEX IF EXISTS {db_schema}.{table}_idx_product_ndc RENAME TO {table}_idx_product_ndc_old;"
    )

    if table == "product":
        await _rename_product_indexes(database, db_schema, import_date)

    await database.status(
        f"ALTER INDEX IF EXISTS {db_schema}.{table}_idx_product_ndc_{import_date} RENAME TO {table}_idx_product_ndc;"
    )

    await database.status(f"ALTER TABLE IF EXISTS {db_schema}.{table} RENAME TO {table}_old;")
    await database.status(f"ALTER TABLE IF EXISTS {db_schema}.{table}_{import_date} RENAME TO {table};")


async def _create_product_indexes(database: Any, db_schema: str, import_date: str) -> None:
    for index in Product.__my_additional_indexes__[1:]:
        await database.status(
            f"CREATE INDEX {index['staging_name']}_{import_date} ON "
            f"{db_schema}.product_{import_date} USING {index['using'].upper()}({', '.join(index['index_elements'])});"
        )


async def _rename_product_indexes(database: Any, db_schema: str, import_date: str) -> None:
    await database.status(
        f"ALTER INDEX IF EXISTS {db_schema}.product_idx_brand_trgm_idx RENAME TO product_idx_brand_trgm_idx_old;"
    )
    await database.status(
        f"ALTER INDEX IF EXISTS {db_schema}.product_idx_generic_trgm_idx RENAME TO product_idx_generic_trgm_idx_old;"
    )
    await database.status(f"ALTER INDEX IF EXISTS {db_schema}.product_rxnorm_idx RENAME TO product_rxnorm_idx_old;")

    await database.status(
        f"ALTER INDEX IF EXISTS "
        f"{db_schema}.product_idx_brand_trgm_idx_{import_date} RENAME TO "
        f"product_idx_brand_trgm_idx;"
    )
    await database.status(
        f"ALTER INDEX IF EXISTS "
        f"{db_schema}.product_idx_generic_trgm_idx_{import_date} RENAME TO "
        f"product_idx_generic_trgm_idx;"
    )
    await database.status(
        f"ALTER INDEX IF EXISTS {db_schema}.product_rxnorm_idx_{import_date} RENAME TO product_rxnorm_idx;"
    )
