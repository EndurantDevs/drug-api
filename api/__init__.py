from sanic.blueprints import Blueprint

from api.control import blueprint as control_blueprint
from api.endpoint.drug import blueprint as v1_drug
from api.endpoint.healthcheck import blueprint as v1_healthcheck
from db.connection import db


def init_api(api):
    """Attach versioned API blueprints and shared database state to Sanic."""
    db.init_app(api)
    api_blueprint = Blueprint.group([v1_healthcheck, v1_drug], version_prefix="/api/v")
    api.blueprint(api_blueprint)
    api.blueprint(control_blueprint)
