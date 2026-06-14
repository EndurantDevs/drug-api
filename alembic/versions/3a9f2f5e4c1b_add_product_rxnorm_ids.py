"""Add rxnorm_ids to product and index for RxNorm lookup.

Revision ID: 3a9f2f5e4c1b
Revises: None
Create Date: 2026-02-13 00:00:00
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "3a9f2f5e4c1b"
down_revision = None
branch_labels = None
depends_on = None


SCHEMA = "rx_data"
TABLE = "product"
INDEX = "product_rxnorm_idx"
COLUMN = "rxnorm_ids"


def _table_exists(inspector):
    return inspector.has_table(TABLE, schema=SCHEMA)


def _column_exists(inspector):
    columns = inspector.get_columns(TABLE, schema=SCHEMA)
    return any(column["name"] == COLUMN for column in columns)


def _index_exists(inspector):
    indexes = inspector.get_indexes(TABLE, schema=SCHEMA)
    return any(index["name"] == INDEX for index in indexes)


def upgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if not _table_exists(inspector):
        return

    if not _column_exists(inspector):
        op.add_column(TABLE, sa.Column(COLUMN, sa.ARRAY(sa.String()), nullable=True), schema=SCHEMA)

    inspector = sa.inspect(bind)
    if not _index_exists(inspector):
        op.create_index(INDEX, TABLE, [COLUMN], schema=SCHEMA, postgresql_using="gin")


def downgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if not _table_exists(inspector):
        return

    if _index_exists(inspector):
        op.drop_index(INDEX, table_name=TABLE, schema=SCHEMA)

    inspector = sa.inspect(bind)
    if _column_exists(inspector):
        op.drop_column(TABLE, COLUMN, schema=SCHEMA)
