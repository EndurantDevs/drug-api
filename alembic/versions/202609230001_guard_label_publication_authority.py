"""Require live Label ownership when changing its publication authority.

Revision ID: 202609230001
Revises: 202609210001
"""

import os
import re

import sqlalchemy as sa

from alembic import op

revision = "202609230001"
down_revision = "202609210001"
branch_labels = None
depends_on = None

# Invoker identity is intentional: a protected heap admits its publisher, while
# ordinary owners can still publish before handing off ownership.
GUARD_BODY = """
BEGIN
    IF (TG_OP <> 'INSERT' AND OLD.importer_id = 'label')
       OR (TG_OP <> 'DELETE' AND NEW.importer_id = 'label') THEN
        IF NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_class relation
            JOIN pg_catalog.pg_namespace namespace ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname = TG_TABLE_SCHEMA AND relation.relname = 'label'
              AND relation.relkind = 'r'
              AND pg_catalog.pg_has_role(current_user, relation.relowner, 'USAGE')
        ) THEN
            RAISE EXCEPTION 'Label publication authority requires live table ownership'
                USING ERRCODE = '42501';
        END IF;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
"""


def _schema():
    schema = os.getenv("DB_SCHEMA") or "rx_data"
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema) or len(schema.encode()) > 63:
        raise RuntimeError("result publication schema is invalid")
    return op.get_bind().dialect.identifier_preparer.quote_schema(schema)


def upgrade():
    """Install after seeded rows; deployment must protect ledger/function ownership.

    At protected handoff, ordinary roles must not own this ledger or function,
    inherit their owners, or retain TRIGGER/TRUNCATE access. SELECT/UPDATE grants
    may remain for the independent drug-indications publication row.
    """
    schema = _schema()
    op.execute(
        sa.text(
            f"CREATE FUNCTION {schema}.guard_label_publication_authority() RETURNS trigger "
            "LANGUAGE plpgsql SECURITY INVOKER SET search_path = pg_catalog, pg_temp "
            f"AS $guard${GUARD_BODY}$guard$"
        )
    )
    op.execute(
        sa.text(
            f"CREATE TRIGGER label_publication_authority_owner BEFORE INSERT OR UPDATE OR DELETE "
            f"ON {schema}.result_publication_authority FOR EACH ROW "
            f"EXECUTE FUNCTION {schema}.guard_label_publication_authority()"
        )
    )


def downgrade():
    """Keep the guard once Label has published; fence concurrent publishers."""
    schema = _schema()
    op.execute(sa.text(f"LOCK TABLE {schema}.result_publication_authority IN ACCESS EXCLUSIVE MODE"))
    pristine = (
        op.get_bind()
        .execute(
            sa.text(
                f"SELECT EXISTS (SELECT 1 FROM {schema}.result_publication_authority "
                "WHERE importer_id='label' AND local_generation=0)"
            )
        )
        .scalar_one()
    )
    if not pristine:
        raise RuntimeError("Label publication evidence prevents guard downgrade")
    op.execute(sa.text(f"DROP TRIGGER label_publication_authority_owner ON {schema}.result_publication_authority"))
    op.execute(sa.text(f"DROP FUNCTION {schema}.guard_label_publication_authority()"))
