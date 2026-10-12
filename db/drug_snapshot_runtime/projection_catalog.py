"""Exact inert native projections; authority admission remains with their owner."""


class ProjectionCatalogError(ValueError):
    """The native catalog differs from the trusted projection declaration."""


def require(condition, reason):
    """Reject a mismatch before a native query can expand the alias."""
    if not condition:
        raise ProjectionCatalogError(reason)


async def require_columns(connection, oid, columns):
    """Exact ordered builtin types, typmods and collations; never expression inference."""
    attributes = await connection.fetch(
        """SELECT a.attnum,a.attname,t.typname,a.atttypmod,
        coll.collname, t.typnamespace='pg_catalog'::regnamespace AND t.typtype<>'d'
        AND (coll.oid IS NULL OR coll.collnamespace='pg_catalog'::regnamespace)
        AND NOT a.attisdropped AND a.attgenerated='' AND a.attidentity=''
        AND a.attacl IS NULL AS safe
        FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_type t ON t.oid=a.atttypid
        LEFT JOIN pg_catalog.pg_collation coll ON coll.oid=a.attcollation
        WHERE a.attrelid=$1 AND a.attnum>0 ORDER BY a.attnum LIMIT 1601""",
        oid,
    )
    require(len(attributes) == len(columns), "columns_changed")
    for position, (actual, expected) in enumerate(zip(attributes, columns, strict=True), 1):
        require(
            tuple(actual)
            == (position, expected.name, expected.type_name, expected.type_modifier, expected.collation_name, True),
            "columns_changed",
        )


async def select_definition(connection, alias, physical):
    """Deparse-normal form for PG18's sole-table explicit-column projection."""
    identifiers = [column.name for column in alias.columns] + [physical["schema_name"], physical["relation_name"]]
    quoted = await connection.fetchval(
        "SELECT pg_catalog.array_agg(pg_catalog.quote_ident(identifier) ORDER BY position) "
        "FROM pg_catalog.unnest($1::text[]) WITH ORDINALITY names(identifier,position)",
        identifiers,
    )
    # PG18 omits needless column qualifiers but retains the fixed table alias.
    return "SELECT " + ", ".join(quoted[:-2]) + " FROM " + ".".join(quoted[-2:]) + " frozen;"


async def view_definition(connection, oid):
    """Deparse with qualified names, then restore the existing transaction's path."""
    require(connection.is_in_transaction(), "transaction_unavailable")
    # Native savepoints also work when an ORM, rather than the driver, owns the transaction.
    await connection.execute("SAVEPOINT ndc_projection_definition")
    try:
        await connection.execute("SET LOCAL search_path = pg_catalog, pg_temp")
        return await connection.fetchval("SELECT pg_catalog.pg_get_viewdef($1::oid,false)", oid)
    finally:
        await connection.execute("ROLLBACK TO SAVEPOINT ndc_projection_definition")
        await connection.execute("RELEASE SAVEPOINT ndc_projection_definition")


async def require_dependencies(connection, oid, namespace_oid, physical_oid, column_count):
    """Admit only the sole projection rule and its exact column-level dependencies."""
    rules = await connection.fetch(
        """SELECT oid,rulename,ev_type::text,ev_enabled::text,is_instead,ev_qual::text
        FROM pg_catalog.pg_rewrite WHERE ev_class=$1 LIMIT 2""",
        oid,
    )
    require(len(rules) == 1 and tuple(rules[0])[1:] == ("_RETURN", "1", "O", True, "<>"), "rules_changed")
    rule_oid = rules[0]["oid"]
    rule_dependencies = await connection.fetch(
        """SELECT objsubid,refclassid::regclass::text,
        refobjid,refobjsubid,deptype::text FROM pg_catalog.pg_depend
        WHERE classid='pg_rewrite'::regclass AND objid=$1 LIMIT 1602""",
        rule_oid,
    )
    expected = {(0, "pg_class", oid, 0, "i")} | {
        (0, "pg_class", physical_oid, position, "n") for position in range(1, column_count + 1)
    }
    require(
        len(rule_dependencies) == len(expected) and {tuple(dependency) for dependency in rule_dependencies} == expected,
        "dependencies_changed",
    )
    await require_relation_dependencies(connection, oid, namespace_oid, rule_oid)


async def require_relation_dependencies(connection, oid, namespace_oid, rule_oid):
    """Refuse external relation, row-type and array-type dependents; never cascade."""
    outgoing = await connection.fetch(
        """SELECT objsubid,refclassid::regclass::text,refobjid,refobjsubid,deptype::text
        FROM pg_catalog.pg_depend WHERE classid='pg_class'::regclass AND objid=$1 LIMIT 3""",
        oid,
    )
    require(
        [tuple(dependency) for dependency in outgoing] == [(0, "pg_namespace", namespace_oid, 0, "n")],
        "dependencies_changed",
    )
    row_type = await connection.fetchval("SELECT reltype FROM pg_catalog.pg_class WHERE oid=$1", oid)
    incoming = await connection.fetch(
        """SELECT classid::regclass::text,objid,objsubid,refobjsubid,deptype::text
        FROM pg_catalog.pg_depend WHERE refclassid='pg_class'::regclass AND refobjid=$1 LIMIT 4""",
        oid,
    )
    expected_dependencies = {("pg_type", row_type, 0, 0, "i"), ("pg_rewrite", rule_oid, 0, 0, "i")}
    require(
        len(incoming) == 2 and {tuple(dependency) for dependency in incoming} == expected_dependencies,
        "dependents_unsupported",
    )
    # A dependent object may name the composite row type instead of the relation itself.
    type_dependents = await connection.fetch(
        """SELECT d.classid::regclass::text,d.objid,d.objsubid,
        d.refobjsubid,d.deptype::text,t.typelem,t.typtype::text
        FROM pg_catalog.pg_depend d LEFT JOIN pg_catalog.pg_type t ON d.classid='pg_type'::regclass AND t.oid=d.objid
        WHERE d.refclassid='pg_type'::regclass AND d.refobjid=$1 LIMIT 2""",
        row_type,
    )
    require(
        len(type_dependents) == 1
        and tuple(type_dependents[0])[0] == "pg_type"
        and tuple(type_dependents[0])[2:] == (0, 0, "i", row_type, "b"),
        "dependents_unsupported",
    )

    require(
        not await connection.fetchval(
            """SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_depend
        WHERE refclassid='pg_type'::regclass AND refobjid=$1)""",
            type_dependents[0]["objid"],
        ),
        "dependents_unsupported",
    )
