import urllib.parse
from datetime import datetime

import sanic.exceptions
from sanic import Blueprint, response

from api.utils import (get_brand_packages, get_brand_products, get_generic_packages, get_generic_products,
                       get_packages_by_rxnorm, get_products_by_rxnorm)
from db.models import DrugConditionEvidence, Label, Package, Product, db

blueprint = Blueprint('drug', url_prefix='/drug', version=1)


@blueprint.get('/')
async def drug_status(request):
    """Return service metadata and current product/package row counts."""
    async def get_product_count():
        """Count product rows available to the API."""
        return await db.select(db.func.count(Product.product_id)).scalar()

    async def get_package_count():
        """Count package rows available to the API."""
        return await db.select(db.func.count(Package.package_ndc)).scalar()

    product_count = await get_product_count()
    package_count = await get_package_count()
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
    """Return one product record by product NDC."""
    data = await Product.query.where(Product.product_ndc == product_ndc).first()
    if data:
        return response.json(data.to_json_dict())
    raise sanic.exceptions.NotFound


@blueprint.get('/ndc/<product_ndc>/packages')
async def product_packages_obj(request, product_ndc):
    """Return packages attached to one product NDC."""
    package_results = []
    package_query = Package.query.where(Package.product_ndc == product_ndc)

    async with db.transaction():
        async for package in package_query.iterate():
            package_results.append(package.to_json_dict())

    return response.json(package_results)


@blueprint.get('/ndc/package/<package_ndc>')
async def package_product_ndc_obj(request, package_ndc):
    """Return one package record with its nested product."""
    data = await Package.query.where(Package.package_ndc == package_ndc).first()
    if data:
        obj = data.to_json_dict()
        data = await Product.query.where(Product.product_ndc == obj['product_ndc']).first()
        obj['product'] = data.to_json_dict()
        return response.json(obj)
    raise sanic.exceptions.NotFound


@blueprint.get('/label/package/<package_ndc>')
async def package_ndc_obj(request, package_ndc):
    """Return one package, nested product, and label when available."""
    data = await Package.query.where(Package.package_ndc == package_ndc).first()
    if data:
        obj = data.to_json_dict()
        data = await Product.query.where(Product.product_ndc == obj['product_ndc']).first()
        obj['product'] = data.to_json_dict()
        data = None
        if 'spl_id' in obj['product'] and obj['product']['spl_id']:
            data = await Label.query.where(Label.id == obj['product']['spl_id']).first()
            if not data:
                data = await Label.query.where(Label.set_id == obj['product']['spl_id']).first()
        if data:
            obj['label'] = data.to_json_dict()
        return response.json(obj)
    raise sanic.exceptions.NotFound


@blueprint.get('/label/product/<product_ndc>')
async def label_product_ndc_obj(request, product_ndc):
    """Return one product and its label when available."""
    data = await Product.query.where(Product.product_ndc == product_ndc).first()
    if data:
        obj = data.to_json_dict()
        data = None
        if 'spl_id' in obj and obj['spl_id']:
            data = await Label.query.where(Label.id == obj['spl_id']).first()
            if not data:
                data = await Label.query.where(Label.set_id == obj['spl_id']).first()
        if data:
            obj['label'] = data.to_json_dict()
        return response.json(obj)
    raise sanic.exceptions.NotFound


@blueprint.get('/list-product/all', name='list_product_all')
@blueprint.get('/list-product/all/<page:int>/', name='list_product_all_with_page')
@blueprint.get('/list-product/all/<page:int>/<results_per_page:int>', name='list_product_all_with_page_and_results_per_page')
async def list_product_all(request, letter='a', page=0, results_per_page = 49999, prefix='', separator='', suffix=''):
    """Return a paginated list of product NDC display names."""
    for (query_key, query_value) in request.query_args:
        if query_key == 'prefix' and query_value:
            prefix = query_value
        elif query_key == 'separator' and query_value:
            separator = query_value
        elif query_key == 'suffix' and query_value:
            suffix = query_value
    if not letter or len(letter) > 1:
        raise sanic.exceptions.NotFound
    if not page or page<0:
        page = 0

    product_options = []
    product_query = db.select([Product.product_ndc, Product.generic_name, Product.brand_name]).order_by(Product.generic_name, Product.brand_name).limit(
        results_per_page).offset(results_per_page * page)


    async with db.transaction():
        async for product_row in product_query.iterate():
            product_name = product_row["generic_name"] or product_row["brand_name"] or ""
            if (
                product_row["brand_name"]
                and product_row["generic_name"]
                and product_row['brand_name'].lower() != product_row['generic_name'].lower()
            ):
                product_name = product_row["brand_name"]

            product_options.append({'product_ndc': product_row['product_ndc'], 'name': product_name})

    if product_options:
        return response.json(product_options)

    raise sanic.exceptions.NotFound


@blueprint.get('/list-product/<letter>', name='list_product_letter')
@blueprint.get('/list-product/<letter>/<page:int>', name='list_product_letter_with_page')
@blueprint.get('/list-product/<letter>/<page:int>/<results_per_page:int>', name='list_product_letter_with_page_and_results_per_page')
async def list_product_letter(request, letter, page=0, results_per_page = 100):
    """Return products whose generic name starts with the requested letter."""
    if not letter or len(letter) > 1:
        raise sanic.exceptions.NotFound
    if not page or page<0:
        page = 0

    product_options = []
    product_query = db.select([Product.product_ndc, Product.generic_name]).where(
        Product.generic_name.ilike(f"{letter}%")).order_by(Product.generic_name, Product.brand_name).limit(
        results_per_page).offset(results_per_page * page)


    async with db.transaction():
        async for product_row in product_query.iterate():
            product_options.append({'product_ndc': product_row['product_ndc'], 'name': product_row['generic_name']})

    if product_options:
        return response.json(product_options)
    raise sanic.exceptions.NotFound

@blueprint.get('/name/<product_name>/products', name='product_data_by_name')
@blueprint.get('/name/<product_name>/generic_products', name='product_data_by_generic_name')
@blueprint.get('/name/<product_name>/brand_products', name='product_data_by_brand_name')
async def product_data_by_name(request, product_name):
    """Return matching generic and brand products for a decoded name."""
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
    """Return matching generic and brand packages for a decoded name."""

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


@blueprint.get('/rxnorm/<rxnorm_id>/products')
async def products_by_rxnorm(request, rxnorm_id):
    """Return products linked to one RxNorm identifier."""
    rxnorm_id = rxnorm_id.strip()
    products = await get_products_by_rxnorm(rxnorm_id)
    if not products:
        raise sanic.exceptions.NotFound
    return response.json(products)


@blueprint.get('/rxnorm/<rxnorm_id>/packages')
async def packages_by_rxnorm(request, rxnorm_id):
    """Return packages linked to one RxNorm identifier."""
    rxnorm_id = rxnorm_id.strip()
    packages = await get_packages_by_rxnorm(rxnorm_id)
    if not packages:
        raise sanic.exceptions.NotFound
    return response.json(packages)


@blueprint.get('/rxnorm/<rxnorm_id>/conditions')
async def conditions_by_rxnorm(request, rxnorm_id):
    """Return condition evidence rows linked to one RxNorm identifier."""
    rxnorm_id = rxnorm_id.strip()
    limit = min(int(request.args.get('limit', 100)), 500)
    evidence_rows = []
    evidence_query = DrugConditionEvidence.query.where(DrugConditionEvidence.rxnorm_ids.contains([rxnorm_id])).limit(limit)
    async with db.transaction():
        async for row in evidence_query.iterate():
            evidence_rows.append(row.to_json_dict())
    if not evidence_rows:
        raise sanic.exceptions.NotFound
    return response.json(evidence_rows)


@blueprint.get('/ndc/<product_ndc>/conditions')
async def conditions_by_product_ndc(request, product_ndc):
    """Return condition evidence rows linked to one product NDC."""
    product_ndc = product_ndc.strip()
    limit = min(int(request.args.get('limit', 100)), 500)
    evidence_rows = []
    evidence_query = DrugConditionEvidence.query.where(DrugConditionEvidence.product_ndc.contains([product_ndc])).limit(limit)
    async with db.transaction():
        async for row in evidence_query.iterate():
            evidence_rows.append(row.to_json_dict())
    if not evidence_rows:
        raise sanic.exceptions.NotFound
    return response.json(evidence_rows)


@blueprint.get('/label/<set_id>/condition-evidence')
async def condition_evidence_by_label(request, set_id):
    """Return condition evidence rows linked to one SPL set id."""
    set_id = set_id.strip()
    evidence_rows = []
    evidence_query = DrugConditionEvidence.query.where(DrugConditionEvidence.set_id == set_id)
    async with db.transaction():
        async for row in evidence_query.iterate():
            evidence_rows.append(row.to_json_dict())
    if not evidence_rows:
        raise sanic.exceptions.NotFound
    return response.json(evidence_rows)
