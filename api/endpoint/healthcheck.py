from datetime import datetime

from asyncpg.exceptions import InterfaceError, PostgresError
from sanic import Blueprint, response
from sqlalchemy.exc import SQLAlchemyError

from db.models import Product

blueprint = Blueprint('healthcheck', url_prefix='/healthcheck', version=1)


@blueprint.get('/')
async def healthcheck(request):
    """Return process metadata plus a simple database connectivity check."""
    database = await _check_db()
    data = {
        'date': datetime.utcnow().isoformat(),
        'release': request.app.config.get('RELEASE'),
        'environment': request.app.config.get('ENVIRONMENT'),
        'database': database
    }

    status = 200 if database['status'] == 'OK' else 503
    return response.json(data, status=status)


@blueprint.get('/live')
async def liveness(request):
    """Confirm that the API process can answer requests."""
    return response.json({
        'status': 'OK',
        'release': request.app.config.get('RELEASE'),
    })


async def _check_db():
    """Probe the Product table to verify database access."""
    try:
        await Product.load(Product.product_id).limit(1).first()
        return {
            'status': 'OK'
        }
    except (PostgresError, InterfaceError, ConnectionRefusedError, SQLAlchemyError) as ex:
        return {
            'status': 'Fail',
            'details': str(ex)
        }
