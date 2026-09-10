"""DDL helpers for publishing staged FDA NDC import tables."""

from typing import Any

from process.ndc_stage import (
    audit_ndc_tables,
    check_ndc_incumbents,
    finish_ndc_publication,
    lock_ndc_stages,
)


async def publish_ndc_tables(database: Any, db_schema: str, import_date: str, *,
                             attempt=None, acquisition=None):
    """Build staged indexes before touching either live table, then publish atomically."""
    await database.status("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
    await database.status("CREATE EXTENSION IF NOT EXISTS btree_gin;")
    async with database.transaction() as session:
        if attempt is not None:
            if attempt.schema != db_schema or attempt.suffix != import_date:
                raise ValueError("NDC publication scope differs from stage owner")
            await lock_ndc_stages(database, attempt, exclusive=True)
            if not acquisition["complete"]:
                receipt = await audit_ndc_tables(database, session, attempt, acquisition)
                return await finish_ndc_publication(database, attempt, receipt, published=False)
        for table in ['product', 'package']:
            print(f'Creating indexes for {table} ...')
            await database.status(
                f"CREATE INDEX {table}_idx_product_ndc_{import_date} ON "
                f"{db_schema}.{table}_{import_date} USING GIN(product_ndc);")
            if table == 'product':
                await _create_product_indexes(database, db_schema, import_date)
        if attempt is not None:
            receipt = await audit_ndc_tables(database, session, attempt, acquisition)
            await check_ndc_incumbents(database, attempt)
        for table in ['product', 'package']:
            await _publish_single_ndc_table(database, db_schema, table, import_date)
        if attempt is not None:
            return await finish_ndc_publication(database, attempt, receipt, published=True)
    return None


async def _publish_single_ndc_table(database: Any, db_schema: str, table: str, import_date: str) -> None:
    await database.status(f"DROP TABLE IF EXISTS {db_schema}.{table}_old;")

    await database.status(f"ALTER INDEX IF EXISTS "
                          f"{db_schema}.{table}_idx_product_ndc RENAME TO "
                          f"{table}_idx_product_ndc_old;")

    if table == 'product':
        await _rename_product_indexes(database, db_schema, import_date)

    await database.status(f"ALTER INDEX IF EXISTS "
                          f"{db_schema}.{table}_idx_product_ndc_{import_date} RENAME TO "
                          f"{table}_idx_product_ndc;")

    await database.status(f"ALTER TABLE IF EXISTS {db_schema}.{table} RENAME TO {table}_old;")
    await database.status(f"ALTER TABLE IF EXISTS {db_schema}.{table}_{import_date} RENAME TO {table};")


async def _create_product_indexes(database: Any, db_schema: str, import_date: str) -> None:
    await database.status(f"CREATE INDEX product_idx_brand_trgm_idx_{import_date} ON "
                          f"{db_schema}.product_{import_date} "
                          f"USING GIN(brand_name gin_trgm_ops);")
    await database.status(f"CREATE INDEX product_idx_generic_trgm_idx_{import_date} ON "
                          f"{db_schema}.product_{import_date} USING "
                          f"GIN(generic_name gin_trgm_ops);")
    await database.status(
        f"CREATE INDEX product_rxnorm_idx_{import_date} ON "
        f"{db_schema}.product_{import_date} USING GIN(rxnorm_ids);"
    )


async def _rename_product_indexes(database: Any, db_schema: str, import_date: str) -> None:
    await database.status(f"ALTER INDEX IF EXISTS "
                          f"{db_schema}.product_idx_brand_trgm_idx RENAME TO "
                          f"product_idx_brand_trgm_idx_old;")
    await database.status(f"ALTER INDEX IF EXISTS "
                          f"{db_schema}.product_idx_generic_trgm_idx RENAME TO "
                          f"product_idx_generic_trgm_idx_old;")
    await database.status(f"ALTER INDEX IF EXISTS "
                          f"{db_schema}.product_rxnorm_idx RENAME TO "
                          f"product_rxnorm_idx_old;")

    await database.status(f"ALTER INDEX IF EXISTS "
                          f"{db_schema}.product_idx_brand_trgm_idx_{import_date} RENAME TO "
                          f"product_idx_brand_trgm_idx;")
    await database.status(f"ALTER INDEX IF EXISTS "
                          f"{db_schema}.product_idx_generic_trgm_idx_{import_date} RENAME TO "
                          f"product_idx_generic_trgm_idx;")
    await database.status(f"ALTER INDEX IF EXISTS "
                          f"{db_schema}.product_rxnorm_idx_{import_date} RENAME TO "
                          f"product_rxnorm_idx;")
