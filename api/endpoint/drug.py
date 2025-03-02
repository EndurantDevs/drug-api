import asyncio
from datetime import datetime
import urllib.parse

import sanic.exceptions
from sanic import response
from sanic import Blueprint

from db.models import db, Product, Package, Label
from api.utils import get_brand_products, get_brand_packages, get_generic_products, get_generic_packages

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
    if data:
        return response.json(data.to_json_dict())
    raise sanic.exceptions.NotFound


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
async def package_product_ndc_obj(request, package_ndc):
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
async def label_product_ndc_obj(request, product_ndc):
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


@blueprint.get('/list-product/all', name='list_product_all')
@blueprint.get('/list-product/all/<page:int>/', name='list_product_all_with_page')
@blueprint.get('/list-product/all/<page:int>/<results_per_page:int>', name='list_product_all_with_page_and_results_per_page')
async def list_product_all(request, letter='a', page=0, results_per_page = 49999, prefix='', separator='', suffix=''):
    for (key, value) in request.query_args:
        if key == 'prefix' and value:
            prefix = value
        elif key == 'separator' and value:
            separator = value
        elif key == 'suffix' and value:
            suffix = value
    if not letter or len(letter) > 1:
        raise sanic.exceptions.NotFound
    if not page or page<0:
        page = 0

    data = []
    q = db.select([Product.product_ndc, Product.generic_name, Product.brand_name]).order_by(Product.generic_name, Product.brand_name).limit(
        results_per_page).offset(results_per_page * page).gino


    async with db.transaction():
        from urllib.parse import quote
        async for res in q.iterate():
            if res["generic_name"]:
                name = res["generic_name"]

            if res["brand_name"] and res["generic_name"] and res['brand_name'].lower() != res['generic_name'].lower():
                name = res["brand_name"]

            obj = {'product_ndc': res['product_ndc'], 'name': name}
            data.append(obj)
            # if name:
            #     data += f'{prefix}{quote(str(name.capitalize()), safe="")}{separator}{res["product_ndc"]}{suffix}\n'

    if data:
        return response.json(data)

    raise sanic.exceptions.NotFound


@blueprint.get('/list-product/<letter>', name='list_product_letter')
@blueprint.get('/list-product/<letter>/<page:int>', name='list_product_letter_with_page')
@blueprint.get('/list-product/<letter>/<page:int>/<results_per_page:int>', name='list_product_letter_with_page_and_results_per_page')
async def list_product_letter(request, letter, page=0, results_per_page = 100):
    if not letter or len(letter) > 1:
        raise sanic.exceptions.NotFound
    if not page or page<0:
        page = 0

    data = []
    q = db.select([Product.product_ndc, Product.generic_name]).where(
        Product.generic_name.ilike(f"{letter}%")).order_by(Product.generic_name, Product.brand_name).limit(
        results_per_page).offset(results_per_page * page).gino


    async with db.transaction():
        async for res in q.iterate():
            obj = {'product_ndc': res['product_ndc'], 'name': res['generic_name']}
            data.append(obj)

    if data:
        return response.json(data)
    raise sanic.exceptions.NotFound

@blueprint.get('/name/<product_name>/products', name='product_data_by_name')
@blueprint.get('/name/<product_name>/generic_products', name='product_data_by_generic_name')
@blueprint.get('/name/<product_name>/brand_products', name='product_data_by_brand_name')
async def product_data_by_name(request, product_name):
    product_name = urllib.parse.unquote(product_name).lower()

    #waiting for a fix with asyncpg to apply asyncio.gather!
    (generic_products, brand_products) = ([],[])
    url = request.url.lower().rstrip('/')
    if url.endswith('/generic_products'):
        generic_products = await get_generic_products(product_name)
    elif url.endswith('/brand_products'):
        brand_products = await get_brand_products(product_name)
    else:
        #waiting for asyncio fix in asyncpg for db.aquire
        generic_products = await get_generic_products(product_name)
        brand_products = await get_brand_products(product_name)

    if not generic_products:
        generic_products = []
    if not brand_products:
        brand_products = []

    return response.json({'generic': generic_products, 'brand': brand_products})


@blueprint.get('/name/<product_name>/packages', name='package_data_by_name')
@blueprint.get('/name/<product_name>/generic_packages', name='package_data_by_generic_name')
@blueprint.get('/name/<product_name>/brand_packages', name='package_data_by_brand_name')
async def package_data_by_name(request, product_name):

    product_name = urllib.parse.unquote(product_name).lower()

    #waiting for a fix with asyncpg to apply asyncio.gather!
    (generic_packages, brand_packages) = ([],[])
    url = request.url.lower().rstrip('/')
    if url.endswith('/generic_packages'):
        generic_packages = await get_generic_packages(product_name)
    elif url.endswith('/brand_packages'):
        brand_packages = await get_brand_packages(product_name)
    else:
        #waiting for asyncio fix in asyncpg for db.aquire
        generic_packages = await get_generic_packages(product_name)
        brand_packages = await get_brand_packages(product_name)

    if not generic_packages:
        generic_packages = []
    if not brand_packages:
        brand_packages = []
    return response.json({'generic': generic_packages, 'brand': brand_packages})
