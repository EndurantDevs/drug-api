"""Add durable authority for transferable result publications.

Revision ID: 202609210001
Revises: 3a9f2f5e4c1b
Create Date: 2026-09-21 00:01:00
"""

from __future__ import annotations

import os
import re
from uuid import uuid4

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "202609210001"
down_revision = "3a9f2f5e4c1b"
branch_labels = None
depends_on = None
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_IMPORTERS = ("label", "drug-indications")
_FORMAT = "drug-result-consumed-dependencies-v1"


def _schema() -> str:
    schema = os.getenv("DB_SCHEMA") or "rx_data"
    if not _IDENTIFIER.fullmatch(schema) or len(schema.encode()) > 63:
        raise RuntimeError("result publication schema is invalid")
    return schema


def _shape_check() -> str:
    return (
        "importer_id IN ('label', 'drug-indications') AND local_generation >= 0 AND ("
        "(local_generation = 0 AND origin_lineage_id IS NULL AND origin_generation IS NULL AND published_at IS NULL "
        "AND relation_oids IS NULL AND consumed_dependencies IS NULL) OR ("
        "local_generation > 0 AND origin_lineage_id IS NOT NULL AND origin_generation IS NOT NULL "
        "AND origin_generation > 0 AND published_at IS NOT NULL "
        "AND relation_oids IS NOT NULL AND cardinality(relation_oids) = 1 "
        "AND array_position(relation_oids, NULL) IS NULL "
        "AND relation_oids[1] > 0 AND relation_oids[1] <= 4294967295 "
        "AND consumed_dependencies IS NOT NULL AND jsonb_typeof(consumed_dependencies) = 'object' AND ("
        "(importer_id = 'label' AND consumed_dependencies = '{}'::jsonb) OR "
        f"(importer_id = 'drug-indications' AND consumed_dependencies->>'format' IS NOT NULL "
        f"AND consumed_dependencies->>'format' = '{_FORMAT}' "
        "AND consumed_dependencies ?& ARRAY['label','ndc','clinical-reference']))))"
    )


def upgrade() -> None:
    """Install generation-zero rows without inventing legacy history."""

    schema = _schema()
    op.create_table(
        "result_publication_authority",
        sa.Column("importer_id", sa.Text(), nullable=False),
        sa.Column("local_lineage_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("local_generation", sa.BigInteger(), nullable=False),
        sa.Column("origin_lineage_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("origin_generation", sa.BigInteger(), nullable=True),
        sa.Column("published_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("relation_oids", postgresql.ARRAY(sa.BigInteger()), nullable=True),
        sa.Column("consumed_dependencies", postgresql.JSONB(), nullable=True),
        sa.CheckConstraint(_shape_check(), name="result_publication_authority_shape_check"),
        sa.PrimaryKeyConstraint("importer_id"),
        schema=schema,
    )
    quoted_schema = op.get_bind().dialect.identifier_preparer.quote_schema(schema)
    for importer_id in _IMPORTERS:
        op.execute(
            sa.text(
                f'INSERT INTO {quoted_schema}."result_publication_authority" '
                "(importer_id, local_lineage_id, local_generation) VALUES (:importer_id, :lineage_id, 0)"
            ).bindparams(
                sa.bindparam("importer_id", value=importer_id),
                sa.bindparam("lineage_id", value=uuid4(), type_=postgresql.UUID(as_uuid=True)),
            )
        )


def downgrade() -> None:
    """Refuse to erase local publication evidence."""

    schema = _schema()
    quoted_schema = op.get_bind().dialect.identifier_preparer.quote_schema(schema)
    op.execute(sa.text(f'LOCK TABLE {quoted_schema}."result_publication_authority" IN ACCESS EXCLUSIVE MODE'))
    retained = op.get_bind().execute(
        sa.text(
            f'SELECT EXISTS (SELECT 1 FROM {quoted_schema}."result_publication_authority" '
            "WHERE local_generation <> 0 OR origin_generation IS NOT NULL)"
        )
    ).scalar_one()
    if retained:
        raise RuntimeError("result publication evidence prevents downgrade")
    op.drop_table("result_publication_authority", schema=schema)
