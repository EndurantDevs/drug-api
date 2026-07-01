"""DDL helpers for publishing staged FDA NDC import tables."""

from typing import Any


async def publish_ndc_tables(database: Any, db_schema: str, import_date: str) -> None:
    """Create indexes and swap staged product/package tables into service."""
    await database.status("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
    await database.status("CREATE EXTENSION IF NOT EXISTS btree_gin;")
    for table in ['product', 'package']:
        async with database.transaction():
            await _publish_ndc_table(database, db_schema, table, import_date)


async def _publish_ndc_table(database: Any, db_schema: str, table: str, import_date: str) -> None:
    print(f'Creating indexes for {table} ...')
    await database.status(
        f"CREATE INDEX {table}_idx_product_ndc_{import_date} ON "
        f"{db_schema}.{table}_{import_date} USING GIN(product_ndc);")

    await database.status(f"DROP TABLE IF EXISTS {db_schema}.{table}_old;")

    if table == 'product':
        await _create_product_indexes(database, db_schema, import_date)

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
