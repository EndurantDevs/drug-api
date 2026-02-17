from db.models import Package, Product, db


async def get_generic_packages(product_name):
    data = []
    low_subq = db.select([Product.generic_name]).select_from(Product).where(
        Product.brand_name.ilike(product_name) | Product.generic_name.ilike(product_name)).group_by(
        Product.generic_name)

    subq = db.select([Product.product_ndc]).select_from(Product).where(
        Product.generic_name.in_(low_subq) & (
                (db.func.lower(Product.generic_name) == db.func.lower(Product.brand_name)) |
                (Product.brand_name == None)))

    q = db.select(
        [Product.generic_name, Product.brand_name, Product.dosage_form, Product.short_dosage_form, Package.package_ndc,
            Package.description, Package.size, Package.size_extra, Package.packages_number,
            Package.package_format]).select_from(
        Product.join(Package, Product.product_ndc == Package.product_ndc)).where(
        Product.product_ndc.in_(subq)).gino

    result_names = ['generic_name', 'brand_name', 'dosage_form', 'short_dosage_form', 'package_ndc',
        'package_description', 'size', 'size_extra', 'packages_number', 'package_format']
    async with db.transaction():
        async for package in q.iterate():
            obj = {}
            for i in range (0, len(package)):
                obj[result_names[i]] = package[i]

            data.append(obj)
    return data


async def get_brand_packages(product_name):
    data = []
    low_subq = db.select([Product.generic_name]).select_from(Product).where(
        Product.brand_name.ilike(product_name) | Product.generic_name.ilike(product_name)).group_by(
        Product.generic_name)

    subq = db.select([Product.product_ndc]).select_from(Product).where(
        Product.generic_name.in_(low_subq) &
        (db.func.lower(Product.generic_name) != db.func.lower(Product.brand_name)) &
        (Product.brand_name != None))

    q = db.select(
        [Product.generic_name, Product.brand_name, Product.dosage_form, Product.short_dosage_form, Package.package_ndc,
            Package.description, Package.size, Package.size_extra, Package.packages_number,
            Package.package_format]).select_from(
        Product.join(Package, Product.product_ndc == Package.product_ndc)).where(
        Product.product_ndc.in_(subq)).gino

    result_names = ['generic_name', 'brand_name', 'dosage_form', 'short_dosage_form', 'package_ndc',
        'package_description', 'size', 'size_extra', 'packages_number', 'package_format']
    async with db.transaction():
        async for package in q.iterate():
            obj = {}
            for i in range (0, len(package)):
                obj[result_names[i]] = package[i]

            data.append(obj)
    return data

async def get_generic_products(product_name):
    data = []
    low_subq = db.select([Product.generic_name]).select_from(Product).where(
        Product.brand_name.ilike(product_name) | Product.generic_name.ilike(product_name)).group_by(
        Product.generic_name)

    subq = db.select([Product.product_ndc]).select_from(Product).where(
        Product.generic_name.in_(low_subq) & (
                (db.func.lower(Product.generic_name) == db.func.lower(Product.brand_name)) |
                (Product.brand_name == None)))

    q = db.select(
        [Product.generic_name, Product.brand_name, Product.dosage_form, Product.product_ndc,
            Product.labeler_name]).where(Product.product_ndc.in_(subq)).order_by(Product.product_ndc.desc()).gino

    result_names = ['generic_name', 'brand_name', 'dosage_form', 'product_ndc', 'labeler_name']
    async with db.transaction():
        async for package in q.iterate():
            obj = {}
            for i in range (0, len(package)):
                obj[result_names[i]] = package[i]
            data.append(obj)
    return data


async def get_brand_products(product_name):
    data = []
    low_subq = db.select([Product.generic_name]).select_from(Product).where(
        Product.brand_name.ilike(product_name) | Product.generic_name.ilike(product_name)).group_by(
        Product.generic_name)

    subq = db.select([Product.product_ndc]).select_from(Product).where(
        Product.generic_name.in_(low_subq) &
        (db.func.lower(Product.generic_name) != db.func.lower(Product.brand_name)) &
        (Product.brand_name != None))

    q = db.select(
        [Product.generic_name, Product.brand_name, Product.dosage_form, Product.product_ndc,
            Product.labeler_name]).where(Product.product_ndc.in_(subq)).order_by(Product.brand_name.desc()).gino

    result_names = ['generic_name', 'brand_name', 'dosage_form', 'product_ndc', 'labeler_name']
    async with db.transaction():
        async for package in q.iterate():
            obj = {}
            for i in range (0, len(package)):
                obj[result_names[i]] = package[i]
            data.append(obj)
    return data


async def get_products_by_rxnorm(rxnorm_id):
    rows = await Product.query.where(Product.rxnorm_ids.contains([rxnorm_id])).gino.all()
    return [row.to_json_dict() for row in rows]


async def get_packages_by_rxnorm(rxnorm_id):
    products = await db.select([Product.product_ndc]).where(Product.rxnorm_ids.contains([rxnorm_id])).gino.all()
    product_ids = [row[0] for row in products if row and row[0]]
    product_ids = [pid for pid in product_ids if pid]
    if not product_ids:
        return []
    packages = await Package.query.where(Package.product_ndc.in_(product_ids)).gino.all()
    return [pkg.to_json_dict() for pkg in packages]
