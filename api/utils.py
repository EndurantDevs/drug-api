from db.models import db, Product, Package


async def get_generic_packages(product_name):
    data = []
    low_subq = db.select([Product.generic_name]).select_from(Product).where(
        Product.brand_name.ilike(product_name) | Product.generic_name.ilike(product_name)).group_by(
        Product.generic_name)

    subq = db.select([Product.product_ndc]).select_from(Product).where(
        Product.generic_name.in_(low_subq) & (
                (db.func.lower(Product.generic_name) == db.func.lower(Product.brand_name)) |
                (Product.brand_name == None)))

    q = db.select([Product.generic_name, Product.brand_name, Product.dosage_form, Package.package_ndc,
                      Package.description]).select_from(
        Product.join(Package, Product.product_ndc == Package.product_ndc)).where(
        Product.product_ndc.in_(subq)).gino

    result_names = ['generic_name', 'brand_name', 'dosage_form', 'package_ndc', 'package_description']
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

    q = db.select([Product.generic_name, Product.brand_name, Product.dosage_form, Package.package_ndc,
                      Package.description]).select_from(
        Product.join(Package, Product.product_ndc == Package.product_ndc)).where(
        Product.product_ndc.in_(subq)).gino

    result_names = ['generic_name', 'brand_name', 'dosage_form', 'package_ndc', 'package_description']
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
