"""DDL helpers for publishing staged FDA label import tables."""

from typing import Any


async def publish_label_table(database: Any, db_schema: str, import_date: str) -> None:
    """Create indexes and swap the staged label table into service."""
    async with database.transaction():
        print('Creating indexes..')
        await database.status(
            f"CREATE INDEX idx_product_ndc_{import_date} ON "
            f"{db_schema}.label_{import_date} USING GIN(product_ndc);")
        await database.status(
            f"CREATE INDEX idx_package_ndc_{import_date} ON "
            f"{db_schema}.label_{import_date} USING GIN(package_ndc);")
        await database.status(
            f"CREATE INDEX idx_label_id_{import_date} ON "
            f"{db_schema}.label_{import_date} (id);")
        await database.status(
            f"CREATE INDEX idx_label_set_id_{import_date} ON "
            f"{db_schema}.label_{import_date} (set_id);")

        await database.status(f"DROP TABLE IF EXISTS {db_schema}.label_old;")

        await database.status(f"ALTER INDEX IF EXISTS "
                              f"{db_schema}.idx_product_ndc RENAME TO idx_product_ndc_old;")
        await database.status(f"ALTER INDEX IF EXISTS "
                              f"{db_schema}.idx_package_ndc RENAME TO idx_package_ndc_old;")
        await database.status(f"ALTER INDEX IF EXISTS "
                              f"{db_schema}.idx_label_id RENAME TO idx_label_id_old;")
        await database.status(f"ALTER INDEX IF EXISTS "
                              f"{db_schema}.idx_label_set_id RENAME TO idx_label_set_id_old;")
        await database.status(f"ALTER TABLE IF EXISTS {db_schema}.label RENAME TO label_old;")

        await database.status(f"ALTER INDEX IF EXISTS "
                              f"{db_schema}.idx_product_ndc_{import_date} RENAME TO idx_product_ndc;")
        await database.status(f"ALTER INDEX IF EXISTS "
                              f"{db_schema}.idx_package_ndc_{import_date} RENAME TO idx_package_ndc;")
        await database.status(f"ALTER INDEX IF EXISTS "
                              f"{db_schema}.idx_label_id_{import_date} RENAME TO idx_label_id;")
        await database.status(f"ALTER INDEX IF EXISTS "
                              f"{db_schema}.idx_label_set_id_{import_date} RENAME TO idx_label_set_id;")
        await database.status(f"ALTER TABLE IF EXISTS "
                              f"{db_schema}.label_{import_date} RENAME TO label;")
