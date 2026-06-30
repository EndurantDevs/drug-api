from db.models import Package, Product, db

PACKAGE_RESULT_FIELDS = [
    'generic_name',
    'brand_name',
    'dosage_form',
    'short_dosage_form',
    'package_ndc',
    'package_description',
    'size',
    'size_extra',
    'packages_number',
    'package_format',
]
PRODUCT_RESULT_FIELDS = ['generic_name', 'brand_name', 'dosage_form', 'product_ndc', 'labeler_name']


def _row_to_result_dict(row_values, field_names):
    return {field_name: row_values[index] for index, field_name in enumerate(field_names)}


def _matching_generic_names_query(product_name):
    return db.select([Product.generic_name]).select_from(Product).where(
        Product.brand_name.ilike(product_name) | Product.generic_name.ilike(product_name)
    ).group_by(Product.generic_name)


def _generic_product_ndcs_query(product_name):
    matching_generic_names_query = _matching_generic_names_query(product_name)
    return db.select([Product.product_ndc]).select_from(Product).where(
        Product.generic_name.in_(matching_generic_names_query.statement)
        & (
            (db.func.lower(Product.generic_name) == db.func.lower(Product.brand_name))
            | Product.brand_name.is_(None)
        )
    )


def _brand_product_ndcs_query(product_name):
    matching_generic_names_query = _matching_generic_names_query(product_name)
    return db.select([Product.product_ndc]).select_from(Product).where(
        Product.generic_name.in_(matching_generic_names_query.statement)
        & (db.func.lower(Product.generic_name) != db.func.lower(Product.brand_name))
        & Product.brand_name.is_not(None)
    )


def _package_rows_query(product_ndcs_query):
    return db.select(
        [
            Product.generic_name,
            Product.brand_name,
            Product.dosage_form,
            Product.short_dosage_form,
            Package.package_ndc,
            Package.description,
            Package.size,
            Package.size_extra,
            Package.packages_number,
            Package.package_format,
        ]
    ).select_from(
        Product.__table__.join(Package.__table__, Product.product_ndc == Package.product_ndc)
    ).where(Product.product_ndc.in_(product_ndcs_query.statement))


def _product_rows_query(product_ndcs_query, order_column):
    return db.select(
        [
            Product.generic_name,
            Product.brand_name,
            Product.dosage_form,
            Product.product_ndc,
            Product.labeler_name,
        ]
    ).where(Product.product_ndc.in_(product_ndcs_query.statement)).order_by(order_column.desc())


async def get_generic_packages(product_name):
    """Return package rows for products whose brand is generic or missing."""
    package_results = []
    package_query = _package_rows_query(_generic_product_ndcs_query(product_name))
    async with db.transaction():
        async for package_row in package_query.iterate():
            package_results.append(_row_to_result_dict(package_row, PACKAGE_RESULT_FIELDS))
    return package_results


async def get_brand_packages(product_name):
    """Return package rows for brand products matching the search term."""
    package_results = []
    package_query = _package_rows_query(_brand_product_ndcs_query(product_name))
    async with db.transaction():
        async for package_row in package_query.iterate():
            package_results.append(_row_to_result_dict(package_row, PACKAGE_RESULT_FIELDS))
    return package_results


async def get_generic_products(product_name):
    """Return generic product rows matching the search term."""
    product_results = []
    product_query = _product_rows_query(_generic_product_ndcs_query(product_name), Product.product_ndc)
    async with db.transaction():
        async for product_row in product_query.iterate():
            product_results.append(_row_to_result_dict(product_row, PRODUCT_RESULT_FIELDS))
    return product_results


async def get_brand_products(product_name):
    """Return brand product rows matching the search term."""
    product_results = []
    product_query = _product_rows_query(_brand_product_ndcs_query(product_name), Product.brand_name)
    async with db.transaction():
        async for product_row in product_query.iterate():
            product_results.append(_row_to_result_dict(product_row, PRODUCT_RESULT_FIELDS))
    return product_results


async def get_products_by_rxnorm(rxnorm_id):
    """Return products mapped to the given RxNorm concept id."""
    rows = await Product.query.where(Product.rxnorm_ids.contains([rxnorm_id])).all()
    return [row.to_json_dict() for row in rows]


def _product_ndc_from_selected_row(row):
    if not row:
        return None
    if isinstance(row, str):
        return row

    value = getattr(row, "product_ndc", None)
    if value:
        return value

    if hasattr(row, "get"):
        value = row.get("product_ndc")
        if value:
            return value

    try:
        return row[0]
    except (KeyError, IndexError, TypeError):
        return None


async def get_packages_by_rxnorm(rxnorm_id):
    """Return packages for products mapped to the given RxNorm concept id."""
    products = await db.select([Product.product_ndc]).where(Product.rxnorm_ids.contains([rxnorm_id])).all()
    product_ids = [_product_ndc_from_selected_row(row) for row in products]
    product_ids = [pid for pid in product_ids if pid]
    if not product_ids:
        return []
    packages = await Package.query.where(Package.product_ndc.in_(product_ids)).all()
    return [pkg.to_json_dict() for pkg in packages]
