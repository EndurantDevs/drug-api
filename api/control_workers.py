from __future__ import annotations

import hashlib
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

from api.control_imports import LABEL_QUEUE_NAME, NDC_QUEUE_NAME
from api.control_worker_manifest import worker_job_manifest, worker_job_name, worker_python


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
    payload: dict[str, Any],
    state: dict[str, Any],
) -> dict[str, Any]:
    image = os.getenv("HLTHPRT_WORKER_JOB_IMAGE", "").strip()
    if not image:
        return {**state, "status": "failed", "message": "HLTHPRT_WORKER_JOB_IMAGE is not configured"}

    namespace = _kubernetes_namespace()
    if state.get("job_status") in {"succeeded", "failed"} and state.get("job_name"):
        try:
            _delete_kubernetes_job(namespace, str(state["job_name"]))
        except _KubernetesApiError as exc:
            if exc.status != 404:
                return {**state, "status": "failed", "message": str(exc)}

    job = worker_job_manifest(spec, payload, image)
    try:
        _kubernetes_request("POST", f"/apis/batch/v1/namespaces/{namespace}/jobs", job)
    except _KubernetesApiError as exc:
        if exc.status == 409:
            refreshed = _worker_state(spec, payload)
            return {**refreshed, "status": "already_running" if refreshed.get("running") else "exists"}
        return {**state, "status": "failed", "message": str(exc)}
    return {**_worker_state(spec, payload), "status": "started"}


def _delete_kubernetes_job(namespace: str, job_name: str) -> None:
    encoded = urllib.parse.quote(job_name, safe="")
    delete_options_dict = {
        "apiVersion": "v1",
        "kind": "DeleteOptions",
        "propagationPolicy": "Background",
    }
    _kubernetes_request("DELETE", f"/apis/batch/v1/namespaces/{namespace}/jobs/{encoded}", delete_options_dict)


def _kubernetes_worker_state(spec: WorkerSpec, worker_request: dict[str, Any] | None = None) -> dict[str, Any]:
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
        return {**base_state_dict, "job_name": worker_job_name(spec, worker_request or {}), "job_status": "unconfigured"}

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
    try:
        response_body_dict = _kubernetes_request("GET", path)
    except _KubernetesApiError as exc:
        return {**base_state_dict, "job_name": worker_job_name(spec, worker_request or {}), "job_status": "error", "message": str(exc)}

    job_items_list = response_body_dict.get("items") if isinstance(response_body_dict, dict) else []
    jobs = [job_item for job_item in job_items_list if isinstance(job_item, dict)]
    active = sum(int((job.get("status") or {}).get("active") or 0) for job in jobs)
    succeeded = sum(int((job.get("status") or {}).get("succeeded") or 0) for job in jobs)
    failed = sum(int((job.get("status") or {}).get("failed") or 0) for job in jobs)
    latest = jobs[-1] if jobs else {}
    latest_name = ((latest.get("metadata") or {}).get("name") if isinstance(latest, dict) else None) or worker_job_name(spec, worker_request or {})
    if active:
        job_status = "active"
    elif failed:
        job_status = "failed"
    elif succeeded:
        job_status = "succeeded"
    else:
        job_status = "missing"
    return {
        **base_state_dict,
        "running": active > 0,
        "job_name": latest_name,
        "job_status": job_status,
        "active_jobs": active,
        "succeeded_jobs": succeeded,
        "failed_jobs": failed,
    }

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
    context = ssl.create_default_context(cafile=str(_K8S_API_CA)) if _K8S_API_CA.exists() else ssl.create_default_context()
    try:
        with urllib.request.urlopen(request, context=context, timeout=10) as response:  # nosec B310 - in-cluster API URL
            raw = response.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise _KubernetesApiError(exc.code, detail or exc.reason) from exc
    except urllib.error.URLError as exc:
        raise _KubernetesApiError(0, str(exc.reason)) from exc
    return json.loads(raw.decode("utf-8")) if raw else {}


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
