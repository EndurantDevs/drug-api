from __future__ import annotations

import asyncio
import hashlib
import http.client
import json
import os
import ssl
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import text

from api.control_imports import LABEL_QUEUE_NAME, NDC_QUEUE_NAME
from api.control_worker_manifest import (
    worker_job_manifest,
    worker_job_name,
    worker_python,
)
from db.connection import db
from process.label_publish import _is_immutable_worker_image, label_publication_protocol
from process.result_publication_authority import schema_name


@dataclass(frozen=True)
class WorkerSpec:
    queue: str
    worker_class: str
    importers: tuple[str, ...]
    role: str = "start"


_WORKERS: tuple[WorkerSpec, ...] = (
    WorkerSpec(NDC_QUEUE_NAME, "process.NDC", ("ndc",)),
    WorkerSpec(LABEL_QUEUE_NAME, "process.Labeling", ("label",)),
    WorkerSpec("arq:queue:drug-api-import-indications", "process.DrugIndications", ("drug-indications",)),
)

_BY_QUEUE = {spec.queue: spec for spec in _WORKERS}
_BY_IMPORTER = {importer: spec for spec in _WORKERS for importer in spec.importers}
_ENGINE_LABEL = "drug"
_K8S_API_TOKEN = Path("/var/run/secrets/kubernetes.io/serviceaccount/token")
_K8S_API_CA = Path("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
_K8S_API_NAMESPACE = Path("/var/run/secrets/kubernetes.io/serviceaccount/namespace")


def worker_registry() -> list[dict[str, Any]]:
    """Return all worker definitions with their current runtime state."""
    return [_worker_state(spec) for spec in _WORKERS]


def ensure_worker(payload: dict[str, Any]) -> dict[str, Any]:
    """Start the requested worker when it is registered and not already running."""
    spec = _resolve_spec(payload)
    if spec is None:
        importer = str(payload.get("importer") or "").strip()
        queue = str(payload.get("queue") or "").strip()
        return {
            "status": "unsupported",
            "items": [],
            "message": f"no worker is registered for {queue or importer or 'request'}",
        }
    item = _ensure_spec(spec, payload)
    return {"status": item["status"], "items": [item]}


async def ensure_worker_request(worker_request_dict: dict[str, Any]) -> dict[str, Any]:
    """Verify an unclaimed queued Label run before replacing its ended worker."""
    spec = _resolve_spec(worker_request_dict)
    if spec is None or spec.worker_class != "process.Labeling" or _launcher_mode() != "kubernetes":
        return ensure_worker(worker_request_dict)
    state_dict = await asyncio.to_thread(_worker_state, spec, worker_request_dict)
    if state_dict["running"]:
        item_dict = {**state_dict, "status": "already_running"}
    elif state_dict.get("job_status") in {"succeeded", "failed"}:
        item_dict = await _replace_unclaimed_label_worker(spec, worker_request_dict, state_dict)
    elif state_dict.get("job_status") in {"pending", "error", "unconfigured"}:
        item_dict = {**state_dict, "status": "exists" if state_dict["job_status"] == "pending" else "failed"}
    else:
        item_dict = await asyncio.to_thread(_ensure_kubernetes_job, spec, worker_request_dict, state_dict)
    return {"status": item_dict["status"], "items": [item_dict]}


async def _replace_unclaimed_label_worker(spec, worker_request_dict, state_dict):
    """Serialize startup repair while retaining all original Job and run evidence."""
    run_id = worker_request_dict.get("run_id")
    schema = schema_name(os.getenv("DB_SCHEMA") or "rx_data")
    try:
        async with asyncio.timeout(15), db.acquire() as connection:
            await connection.status("SET LOCAL lock_timeout = '5s'")
            await connection.status("SET LOCAL statement_timeout = '5s'")
            run_row_dict = await connection.first(
                text(
                    f"SELECT * FROM {schema}.import_run "
                    "WHERE run_id=:run_id AND engine='drug-api' AND importer='label' FOR UPDATE"
                ),
                run_id=run_id if type(run_id) is str else None,
            )
            _require_unclaimed_label_run(run_row_dict, run_id, schema)
            refreshed_dict = await asyncio.to_thread(_worker_state, spec, worker_request_dict)
            if refreshed_dict["running"] or refreshed_dict.get("job_status") == "pending":
                return {**refreshed_dict, "status": "already_running" if refreshed_dict["running"] else "exists"}
            if (
                refreshed_dict.get("terminal_status") not in {"failed", "succeeded"}
                or refreshed_dict.get("active_jobs") != 0
                or refreshed_dict.get("run_id") != run_id
                or not refreshed_dict.get("job_name")
                or not refreshed_dict.get("job_uid")
            ):
                raise RuntimeError("Label startup repair requires the exact terminal worker")
            return await _settle_label_job_creation(spec, worker_request_dict, refreshed_dict)
    except Exception as exc:
        return {**state_dict, "status": "failed", "message": str(exc)}


async def _settle_label_job_creation(spec, worker_request_dict, state_dict):
    """Retain admission ownership until the bounded HTTP create and lookup settle."""
    creation = asyncio.create_task(
        asyncio.to_thread(
            _ensure_kubernetes_job,
            spec,
            worker_request_dict,
            state_dict,
            previous_job_uid=state_dict["job_uid"],
        )
    )
    is_canceled = False
    while True:
        try:
            result_dict = await asyncio.shield(creation)
            break
        except asyncio.CancelledError:
            is_canceled = True
            if creation.done():
                result_dict = creation.result()
                break
    if is_canceled:
        raise asyncio.CancelledError("Label worker creation settled before releasing its admission")
    return result_dict


def _require_unclaimed_label_run(run_row_dict, run_id, schema):
    """Require one queued admission with no runtime claim or publication identity."""
    node_id = os.getenv("HLTHPRT_IMPORT_NODE_ID") or None
    metrics_dict = dict((run_row_dict or {}).get("metrics") or {})
    progress_dict = dict((run_row_dict or {}).get("progress") or {})
    if (
        type(run_id) is not str
        or not run_id
        or run_row_dict is None
        or run_row_dict["run_id"] != run_id
        or run_row_dict["status"] != "queued"
        or any(run_row_dict.get(key) is not None for key in ("started_at", "finished_at", "error"))
        or any(progress_dict.get(key) not in {None, 0} for key in ("done", "pct"))
        or (node_id is not None and run_row_dict["node_id"] != node_id)
        or any(key in metrics_dict for key in ("label_worker_claim", "label_publication_intent", "label_publication"))
        or metrics_dict.get("label_publication_protocol")
        != label_publication_protocol(run_id, schema, run_row_dict["node_id"])
    ):
        raise RuntimeError("Label startup repair requires an unclaimed queued admission")


def _resolve_spec(payload: dict[str, Any]) -> WorkerSpec | None:
    queue = str(payload.get("queue") or "").strip()
    if queue:
        return _BY_QUEUE.get(queue)
    importer = str(payload.get("importer") or "").strip()
    return _BY_IMPORTER.get(importer)


def _ensure_spec(spec: WorkerSpec, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    state = _worker_state(spec, payload)
    if state["running"]:
        return {**state, "status": "already_running"}
    if _launcher_mode() == "kubernetes":
        return _ensure_kubernetes_job(spec, payload or {}, state)
    try:
        pid = _start_process(spec)
    except Exception as exc:
        return {**state, "status": "failed", "message": str(exc)}
    return {**_worker_state(spec), "status": "started", "pid": pid}


def _start_process(spec: WorkerSpec) -> int:
    state_dir = _state_dir()
    log_dir = _log_dir()
    state_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)
    command_args = [sys.executable, str(_main_path()), "worker", spec.worker_class, "--burst"]
    with _log_path(spec).open("ab") as log_handle:
        process = subprocess.Popen(
            command_args,
            cwd=str(_repo_root()),
            env=os.environ.copy(),
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    _pid_path(spec).write_text(str(process.pid), encoding="utf-8")
    return process.pid


def _worker_state(spec: WorkerSpec, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    if _launcher_mode() == "kubernetes":
        return _kubernetes_worker_state(spec, payload)

    pid = _read_pid(_pid_path(spec))
    running = _pid_running(pid) and _pid_matches_current_node(pid)
    if pid and not running:
        _remove_stale_pid(spec)
    if not running:
        pid = _find_running_pid(spec)
        running = _pid_running(pid)
        if running and pid:
            _cache_discovered_pid(spec, pid)
    return {
        "queue": spec.queue,
        "worker_class": spec.worker_class,
        "importers": list(spec.importers),
        "role": spec.role,
        "running": running,
        "pid": pid if running else None,
        "pid_path": str(_pid_path(spec)),
        "log_path": str(_log_path(spec)),
        "command": " ".join([sys.executable, str(_main_path()), "worker", spec.worker_class, "--burst"]),
    }


def _launcher_mode() -> str:
    return os.getenv("HLTHPRT_WORKER_LAUNCHER", "process").strip().lower()


def _ensure_kubernetes_job(
    spec: WorkerSpec,
    worker_request_dict: dict[str, Any],
    state: dict[str, Any],
    *,
    previous_job_uid: str | None = None,
) -> dict[str, Any]:
    image = os.getenv("HLTHPRT_WORKER_JOB_IMAGE", "").strip()
    if not image:
        return {**state, "status": "failed", "message": "HLTHPRT_WORKER_JOB_IMAGE is not configured"}

    namespace = _kubernetes_namespace()
    if state.get("job_status") in {"succeeded", "failed"} and state.get("job_name"):
        if spec.worker_class == "process.Labeling":
            if state["job_name"] == worker_job_name(spec, worker_request_dict, previous_job_uid=previous_job_uid):
                return {
                    **state,
                    "status": "exists",
                    "message": "Terminal Label Job is retained for recovery. Reconcile any claimed run separately. "
                    "A new attempt requires a distinct run.",
                }
        else:
            try:
                _delete_kubernetes_job(namespace, str(state["job_name"]))
            except _KubernetesApiError as exc:
                if exc.status != 404:
                    return {**state, "status": "failed", "message": str(exc)}

    if spec.worker_class == "process.Labeling" and not _is_immutable_worker_image(image):
        return {**state, "status": "failed", "message": "Label worker requires an immutable SHA-256 image"}
    job = worker_job_manifest(spec, worker_request_dict, image, previous_job_uid=previous_job_uid)
    try:
        _kubernetes_request("POST", f"/apis/batch/v1/namespaces/{namespace}/jobs", job)
    except _KubernetesApiError as exc:
        if spec.worker_class == "process.Labeling" and (exc.status in {0, 408, 409} or exc.status >= 500):
            return _reconcile_label_job_creation(state, job, namespace)
        if exc.status == 409:
            refreshed = _worker_state(spec, worker_request_dict)
            return {**refreshed, "status": "already_running" if refreshed.get("running") else "exists"}
        return {**state, "status": "failed", "message": str(exc)}
    return {**_worker_state(spec, worker_request_dict), "status": "started"}


def _reconcile_label_job_creation(state_dict, expected_job_dict, namespace):
    """Resolve an uncertain create by its exact name without issuing another POST."""
    metadata_dict = expected_job_dict["metadata"]
    name = metadata_dict["name"]
    try:
        observed_dict = _kubernetes_request(
            "GET", f"/apis/batch/v1/namespaces/{namespace}/jobs/{urllib.parse.quote(name, safe='')}"
        )
    except _KubernetesApiError:
        return {**state_dict, "status": "failed", "message": "Label worker creation outcome remains unconfirmed"}
    matching_jobs = _matching_worker_jobs(observed_dict, metadata_dict["labels"], name)
    observed_state_dict = _worker_jobs_state(matching_jobs, name, label_jobs=True)
    expected_image = expected_job_dict["spec"]["template"]["spec"]["containers"][0]["image"]
    if (
        not observed_state_dict["job_uid"]
        or observed_state_dict["run_id"] != (metadata_dict.get("annotations") or {}).get("healthporta.com/run-id")
        or observed_state_dict["worker_image"] != expected_image
    ):
        return {**state_dict, "status": "failed", "message": "Label worker creation identity remains unconfirmed"}
    return {
        **state_dict,
        **observed_state_dict,
        "status": "already_running" if observed_state_dict["running"] else "exists",
    }


def _delete_kubernetes_job(namespace: str, job_name: str) -> None:
    encoded = urllib.parse.quote(job_name, safe="")
    delete_options_dict = {
        "apiVersion": "v1",
        "kind": "DeleteOptions",
        "propagationPolicy": "Background",
    }
    _kubernetes_request("DELETE", f"/apis/batch/v1/namespaces/{namespace}/jobs/{encoded}", delete_options_dict)


def _kubernetes_worker_state(
    spec: WorkerSpec, worker_request: dict[str, Any] | None = None, *, claimed_job_name: str | None = None
) -> dict[str, Any]:
    base_state_dict = {
        "queue": spec.queue,
        "worker_class": spec.worker_class,
        "importers": list(spec.importers),
        "role": spec.role,
        "running": False,
        "pid": None,
        "launcher": "kubernetes",
        "command": " ".join([worker_python(), str(_main_path()), "worker", spec.worker_class, "--burst"]),
    }
    if not _kubernetes_configured():
        return {
            **base_state_dict,
            "job_name": worker_job_name(spec, worker_request or {}),
            "job_status": "unconfigured",
        }

    selector_labels_dict = {
        "app.kubernetes.io/managed-by": "healthporta-worker-launcher",
        "healthporta.com/engine": _ENGINE_LABEL,
        "healthporta.com/worker-class-hash": _label_hash(spec.worker_class),
        "healthporta.com/role": spec.role,
    }
    run_id = str((worker_request or {}).get("run_id") or "").strip()
    if run_id:
        selector_labels_dict["healthporta.com/run-id-hash"] = _label_hash(run_id)
    selector = ",".join(f"{label_key}={label_value}" for label_key, label_value in selector_labels_dict.items())
    namespace = _kubernetes_namespace()
    path = f"/apis/batch/v1/namespaces/{namespace}/jobs?{urllib.parse.urlencode({'labelSelector': selector})}"
    if claimed_job_name:
        path = f"/apis/batch/v1/namespaces/{namespace}/jobs/{urllib.parse.quote(claimed_job_name, safe='')}"
    try:
        response_body_dict = _kubernetes_request("GET", path)
    except _KubernetesApiError as exc:
        return {
            **base_state_dict,
            "job_name": worker_job_name(spec, worker_request or {}),
            "job_status": "error",
            "message": str(exc),
        }

    jobs = _matching_worker_jobs(response_body_dict, selector_labels_dict, claimed_job_name)
    is_label = spec.worker_class == "process.Labeling"
    if is_label:
        jobs = _ordered_label_jobs(spec, jobs)
    return {
        **base_state_dict,
        **_worker_jobs_state(jobs, worker_job_name(spec, worker_request or {}), label_jobs=is_label),
    }


def _ordered_label_jobs(spec, jobs):
    """Prefer a replacement over its predecessor even when creation times are equal."""
    job_names_set = {(job.get("metadata") or {}).get("name") for job in jobs}

    def order(job):
        """Sort chain leaves after their predecessors, then use creation time."""
        metadata_dict = job.get("metadata") or {}
        request_dict = {"run_id": (metadata_dict.get("annotations") or {}).get("healthporta.com/run-id")}
        successor_name = worker_job_name(spec, request_dict, previous_job_uid=metadata_dict.get("uid"))
        return (
            successor_name not in job_names_set,
            metadata_dict.get("creationTimestamp") or "",
            metadata_dict.get("name") or "",
        )

    return sorted(jobs, key=order)


def _matching_worker_jobs(response, selector_labels_dict, claimed_job_name):
    """Match a claimed Job's actual name and worker scope without trusting its launch run."""
    if claimed_job_name:
        metadata = response.get("metadata") or {}
        matching = metadata.get("name") == claimed_job_name and all(
            (metadata.get("labels") or {}).get(key) == value for key, value in selector_labels_dict.items()
        )
        return [response] if matching else []
    job_items_list = (response.get("items") or []) if isinstance(response, dict) else []
    return [job_item for job_item in job_items_list if isinstance(job_item, dict)]


def _worker_jobs_state(jobs: list[dict[str, Any]], requested_name: str, *, label_jobs: bool = False) -> dict[str, Any]:
    """Keep aggregate worker state and the actual latest Job identity together."""
    active = sum(int((job.get("status") or {}).get("active") or 0) for job in jobs)
    succeeded = sum(int((job.get("status") or {}).get("succeeded") or 0) for job in jobs)
    failed = sum(int((job.get("status") or {}).get("failed") or 0) for job in jobs)
    latest = jobs[-1] if jobs else {}
    latest_name = (latest.get("metadata") or {}).get("name") or requested_name
    if active:
        job_status = "active"
    elif failed:
        job_status = "failed"
    elif succeeded:
        job_status = "succeeded"
    else:
        job_status = "missing"
    metadata = latest.get("metadata") or {}
    annotations = metadata.get("annotations") or {}
    terminal = _terminal_job_status(latest)
    if label_jobs and not active:
        job_status = terminal or "missing"
        if any(_terminal_job_status(job) is None for job in jobs):
            job_status = "pending"
    containers = (((latest.get("spec") or {}).get("template") or {}).get("spec") or {}).get("containers") or []
    image = next((container.get("image") for container in containers if container.get("name") == "worker"), None)
    return {
        "running": active > 0,
        "job_name": latest_name,
        "job_status": job_status,
        "active_jobs": active,
        "succeeded_jobs": succeeded,
        "failed_jobs": failed,
        "run_id": annotations.get("healthporta.com/run-id"),
        "job_uid": metadata.get("uid"),
        "terminal_status": terminal,
        "worker_image": image,
    }


def _terminal_job_status(job):
    """Require a completed Job condition before treating a worker as terminal."""
    conditions = (job.get("status") or {}).get("conditions") or []
    return next(
        (
            {"Complete": "succeeded", "Failed": "failed"}[condition["type"]]
            for condition in conditions
            if condition.get("status") == "True" and condition.get("type") in {"Complete", "Failed"}
        ),
        None,
    )


def _kubernetes_namespace() -> str:
    override = os.getenv("HLTHPRT_WORKER_JOB_NAMESPACE", "").strip()
    if override:
        return override
    try:
        return _K8S_API_NAMESPACE.read_text(encoding="utf-8").strip()
    except OSError:
        return "default"


def _is_kubernetes_configured() -> bool:
    return bool(os.getenv("KUBERNETES_SERVICE_HOST")) and _K8S_API_TOKEN.exists()


_kubernetes_configured = _is_kubernetes_configured


class _KubernetesApiError(RuntimeError):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status


def _kubernetes_request(method: str, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
    host = os.getenv("KUBERNETES_SERVICE_HOST", "").strip()
    port = os.getenv("KUBERNETES_SERVICE_PORT", "443").strip()
    if not host:
        raise _KubernetesApiError(0, "KUBERNETES_SERVICE_HOST is not configured")
    try:
        token = _K8S_API_TOKEN.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise _KubernetesApiError(0, f"cannot read Kubernetes service account token: {exc}") from exc
    request_body_bytes = None if body is None else json.dumps(body).encode("utf-8")
    request = urllib.request.Request(
        f"https://{host}:{port}{path}",
        data=request_body_bytes,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
    )
    context = (
        ssl.create_default_context(cafile=str(_K8S_API_CA)) if _K8S_API_CA.exists() else ssl.create_default_context()
    )
    try:
        with urllib.request.urlopen(request, context=context, timeout=10) as response:  # nosec B310 - in-cluster API URL
            raw = response.read()
    except urllib.error.HTTPError as exc:
        try:
            detail = exc.read().decode("utf-8", errors="replace")
        except (OSError, http.client.HTTPException) as read_error:
            raise _KubernetesApiError(0, "Kubernetes error response could not be read") from read_error
        raise _KubernetesApiError(exc.code, detail or exc.reason) from exc
    except urllib.error.URLError as exc:
        raise _KubernetesApiError(0, str(exc.reason)) from exc
    except (OSError, http.client.HTTPException) as exc:
        raise _KubernetesApiError(0, str(exc)) from exc
    try:
        result_dict = json.loads(raw.decode("utf-8")) if raw else {}
    except (ValueError, UnicodeError) as exc:
        raise _KubernetesApiError(0, "Kubernetes response could not be decoded") from exc
    if not isinstance(result_dict, dict):
        raise _KubernetesApiError(0, "Kubernetes response is not an object")
    return result_dict


def _label_hash(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:16]


def _read_pid(path: Path) -> int | None:
    try:
        raw = path.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _is_pid_running(pid: int | None) -> bool:
    if not pid or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


_pid_running = _is_pid_running


def _is_pid_on_current_node(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        output = subprocess.check_output(["ps", "eww", "-p", str(pid), "-o", "command="], text=True)
    except Exception:
        try:
            output = subprocess.check_output(["ps", "-p", str(pid), "-o", "command="], text=True)
        except Exception:
            return True
    return _matches_current_node(output)


_pid_matches_current_node = _is_pid_on_current_node


def _find_running_pid(spec: WorkerSpec) -> int | None:
    try:
        output = subprocess.check_output(["ps", "eww", "-axo", "pid=,command="], text=True)
    except Exception:
        try:
            output = subprocess.check_output(["ps", "-axo", "pid=,command="], text=True)
        except Exception:
            return None
    needle = f"main.py worker {spec.worker_class}"
    for line in output.splitlines():
        text = line.strip()
        if needle not in text or " rg " in text:
            continue
        raw_pid = text.split(None, 1)[0]
        try:
            pid = int(raw_pid)
        except ValueError:
            continue
        if pid != os.getpid() and _matches_current_node(text):
            return pid
    return None


def _is_process_on_current_node(process_text: str) -> bool:
    node_id = os.getenv("HLTHPRT_IMPORT_NODE_ID", "").strip()
    if not node_id:
        return True
    key = "HLTHPRT_IMPORT_NODE_ID="
    if key not in process_text:
        return True
    return f"{key}{node_id}" in process_text


_matches_current_node = _is_process_on_current_node


def _cache_discovered_pid(spec: WorkerSpec, pid: int) -> None:
    with suppress(OSError):
        _state_dir().mkdir(parents=True, exist_ok=True)
        _pid_path(spec).write_text(str(pid), encoding="utf-8")


def _remove_stale_pid(spec: WorkerSpec) -> None:
    with suppress(OSError):
        _pid_path(spec).unlink()


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _main_path() -> Path:
    return _repo_root() / "main.py"


def _state_dir() -> Path:
    return Path(os.getenv("HLTHPRT_WORKER_STATE_DIR") or "/tmp/healthporta-workers/drug").resolve()


def _log_dir() -> Path:
    return Path(os.getenv("HLTHPRT_WORKER_LOG_DIR") or "/tmp/healthporta-workers/drug/logs").resolve()


def _safe_name(value: str) -> str:
    return value.replace(":", "_").replace(".", "_").replace("/", "_")


def _pid_path(spec: WorkerSpec) -> Path:
    return _state_dir() / f"{_safe_name(spec.worker_class)}.pid"


def _log_path(spec: WorkerSpec) -> Path:
    return _log_dir() / f"{_safe_name(spec.worker_class)}.log"
