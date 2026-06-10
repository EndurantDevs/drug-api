from __future__ import annotations

import hmac
import os

from sanic import Blueprint, response
from sanic.exceptions import BadRequest, NotFound, SanicException

from api.control_imports import (create_import_run, get_import_run, importer_registry, list_import_runs, node_health,
                                 request_cancel, retry_import_run)
from api.control_workers import ensure_worker, worker_registry

blueprint = Blueprint("control", url_prefix="/control/v1")


@blueprint.exception(SanicException)
async def control_error(request, exc):
    return response.json({"error": {"code": "control_error", "message": str(exc), "request_id": getattr(request, "id", None)}}, status=getattr(exc, "status_code", 500))


def _require_control_auth(request):
    expected = str(os.getenv("HLTHPRT_CONTROL_API_TOKEN") or "").strip()
    if not expected:
        return _auth_error("control API token is required")
    headers = getattr(request, "headers", {}) or {}
    auth_header = str(headers.get("Authorization", ""))
    bearer = auth_header.removeprefix("Bearer ").strip() if auth_header.startswith("Bearer ") else ""
    explicit = str(headers.get("X-HealthPorta-Control-Token", "")).strip()
    if not (hmac.compare_digest(bearer, expected) or hmac.compare_digest(explicit, expected)):
        return _auth_error("control API token is invalid")
    return None


def _auth_error(message: str):
    return response.json({"error": {"code": "forbidden", "message": message}}, status=403)


@blueprint.get("/importers")
async def control_importers(request):
    if auth_error := _require_control_auth(request):
        return auth_error
    return response.json({"items": importer_registry(), "next_cursor": None})


@blueprint.get("/health/node")
async def control_node_health(request):
    if auth_error := _require_control_auth(request):
        return auth_error
    return response.json(node_health(), default=str)


@blueprint.get("/workers")
async def control_workers(request):
    if auth_error := _require_control_auth(request):
        return auth_error
    return response.json({"items": worker_registry(), "next_cursor": None}, default=str)


@blueprint.post("/workers/ensure")
async def control_ensure_worker(request):
    if auth_error := _require_control_auth(request):
        return auth_error
    payload = request.json if isinstance(request.json, dict) else {}
    return response.json(ensure_worker(payload), status=202, default=str)


@blueprint.post("/imports")
async def control_create_import(request):
    if auth_error := _require_control_auth(request):
        return auth_error
    payload = request.json if isinstance(request.json, dict) else {}
    try:
        run, created = await create_import_run(payload)
    except ValueError as exc:
        raise BadRequest(str(exc)) from exc
    return response.json(run, status=201 if created else 409, default=str)


@blueprint.get("/imports")
async def control_list_imports(request):
    if auth_error := _require_control_auth(request):
        return auth_error
    runs = await list_import_runs(status=request.args.get("status"), importer=request.args.get("importer"), limit=int(request.args.get("limit") or 50))
    return response.json({"items": runs, "next_cursor": None}, default=str)


@blueprint.get("/imports/<run_id>")
async def control_get_import(request, run_id):
    if auth_error := _require_control_auth(request):
        return auth_error
    run = await get_import_run(run_id)
    if not run:
        raise NotFound("import run not found")
    return response.json(run, default=str)


@blueprint.post("/imports/<run_id>/cancel")
async def control_cancel_import(request, run_id):
    if auth_error := _require_control_auth(request):
        return auth_error
    try:
        run = await request_cancel(run_id)
    except ValueError as exc:
        raise BadRequest(str(exc)) from exc
    if not run:
        raise NotFound("import run not found")
    return response.json(run, status=202, default=str)


@blueprint.post("/imports/<run_id>/retry")
async def control_retry_import(request, run_id):
    if auth_error := _require_control_auth(request):
        return auth_error
    payload = request.json if isinstance(request.json, dict) else {}
    result = await retry_import_run(run_id, payload)
    if result is None:
        raise NotFound("import run not found")
    run, created = result
    return response.json(run, status=201 if created else 409, default=str)
