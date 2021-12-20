from sanic.blueprints import Blueprint
from db.connection import db
from api.endpoint.healthcheck import blueprint as v1_healthcheck
from api.endpoint.drug import blueprint as v1_drug


def init_api(api):
    db.init_app(api)
    api_bluenprint = Blueprint.group([v1_healthcheck, v1_drug], version_prefix="/api/v")
    api.blueprint(api_bluenprint)