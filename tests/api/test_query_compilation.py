from sqlalchemy.dialects import postgresql

from api.utils import Package, Product, db


def test_name_search_subqueries_compile_with_sqlalchemy_adapter():
    low_subq = db.select([Product.generic_name]).select_from(Product).where(
        Product.brand_name.ilike("aspirin") | Product.generic_name.ilike("aspirin")
    ).group_by(Product.generic_name)

    subq = db.select([Product.product_ndc]).select_from(Product).where(
        Product.generic_name.in_(low_subq.statement) & (
            (db.func.lower(Product.generic_name) == db.func.lower(Product.brand_name)) |
            (Product.brand_name == None)
        )
    )

    query = db.select(
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
    ).where(
        Product.product_ndc.in_(subq.statement)
    )

    compiled = str(query.statement.compile(dialect=postgresql.dialect()))

    assert "SELECT" in compiled
    assert "JOIN" in compiled
    assert "product_ndc IN" in compiled
