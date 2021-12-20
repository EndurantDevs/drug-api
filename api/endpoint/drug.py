import asyncio
from datetime import datetime
import urllib.parse

import sanic.exceptions
from sanic import response
from sanic import Blueprint

from db.models import db, Product, Package, Label

blueprint = Blueprint('drug', url_prefix='/drug', version=1)


@blueprint.get('/')
async def drug_status(request):
    async def get_product_count():
        async with db.acquire():
            return await db.func.count(Product.product_id).gino.scalar()

    async def get_package_count():
        async with db.acquire():
            return await db.func.count(Package.package_ndc).gino.scalar()

    product_count, package_count = await asyncio.gather(get_product_count(), get_package_count())
    data = {
        'date': datetime.utcnow().isoformat(),
        'release': request.app.config.get('RELEASE'),
        'environment': request.app.config.get('ENVIRONMENT'),
        'product_count': product_count,
        'package_count': package_count,
    }

    return response.json(data)


@blueprint.get('/ndc/<product_ndc>')
async def product_ndc_obj(request, product_ndc):
    data = await Product.query.where(Product.product_ndc == product_ndc).gino.first()

    return response.json(data.to_json_dict())


@blueprint.get('/ndc/<product_ndc>/packages')
async def product_packages_obj(request, product_ndc):
    data = []

    q = Package.query.where(Package.product_ndc == product_ndc).gino
    #
    #
    #     db.select([
    #     User,
    #     visits,
    # ]).select_from(
    #     User.outerjoin(Visit)
    # ).group_by(
    #     *User,
    # ).gino.load((User, ColumnLoader(visits)))

    async with db.transaction():
        async for package in q.iterate():
            data.append(package.to_json_dict())

    return response.json(data)


@blueprint.get('/ndc/package/<package_ndc>')
async def product_ndc_obj(request, package_ndc):
    data = await Package.query.where(Package.package_ndc == package_ndc).gino.first()
    if data:
        obj = data.to_json_dict()
        data = await Product.query.where(Product.product_ndc == obj['product_ndc']).gino.first()
        obj['product'] = data.to_json_dict()
        return response.json(obj)
    raise sanic.exceptions.NotFound


@blueprint.get('/label/package/<package_ndc>')
async def package_ndc_obj(request, package_ndc):
    data = await Package.query.where(Package.package_ndc == package_ndc).gino.first()
    if data:
        obj = data.to_json_dict()
        data = await Product.query.where(Product.product_ndc == obj['product_ndc']).gino.first()
        obj['product'] = data.to_json_dict()
        data = None
        if 'spl_id' in obj['product'] and obj['product']['spl_id']:
            data = await Label.query.where(Label.id == obj['product']['spl_id']).gino.first()
            if not data:
                data = await Label.query.where(Label.set_id == obj['product']['spl_id']).gino.first()
        if data:
            obj['label'] = data.to_json_dict()
        return response.json(obj)
    raise sanic.exceptions.NotFound


@blueprint.get('/label/product/<product_ndc>')
async def product_ndc_obj(request, product_ndc):
    data = await Product.query.where(Product.product_ndc == product_ndc).gino.first()
    if data:
        obj = data.to_json_dict()
        data = None
        if 'spl_id' in obj and obj['spl_id']:
            data = await Label.query.where(Label.id == obj['spl_id']).gino.first()
            if not data:
                data = await Label.query.where(Label.set_id == obj['spl_id']).gino.first()
        if data:
            obj['label'] = data.to_json_dict()
        return response.json(obj)
    raise sanic.exceptions.NotFound


@blueprint.get('/name/<product_name>/products')
async def product_data_by_name(request, product_name):

    product_name = urllib.parse.unquote(product_name).lower()

    async def get_generic_products():
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

    async def get_brand_products():
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

    (generic_products, brand_products) = (await get_generic_products(), await get_brand_products())

    if not generic_products:
        generic_products = []
    if not brand_products:
        brand_products = []

    return response.json({'generic': generic_products, 'brand': brand_products})


@blueprint.get('/name/<product_name>/packages')
async def package_data_by_name(request, product_name):

    product_name = urllib.parse.unquote(product_name).lower()

    async def get_generic_packages():
        async with db.acquire():
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

    async def get_brand_packages():
        async with db.acquire():
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

    #waiting for a fix with asyncpg to apply asyncio.gather!
    (generic_packages, brand_packages) = (await get_generic_packages(), await get_brand_packages())
    if not generic_packages:
        generic_packages = []
    if not brand_packages:
        brand_packages = []
    return response.json({'generic': generic_packages, 'brand': brand_packages})
