"""Kubernetes Job manifest builders for import workers."""

import hashlib
import os
from pathlib import Path
from typing import Any

_ENGINE_LABEL = "drug"


def worker_job_manifest(spec: Any, payload: dict[str, Any], image: str) -> dict[str, Any]:
    """Build the Kubernetes Job manifest for one import worker."""
    job_name = worker_job_name(spec, payload)
    run_id = str(payload.get("run_id") or "").strip()
    volumes_list = _worker_job_pvc_volumes()
    labels_dict = _worker_job_labels(spec, run_id)
    container_dict = _worker_job_container(spec, payload, image, volumes_list)
    pod_spec_dict = _worker_job_pod_spec(container_dict, volumes_list)
    job_spec_dict = _worker_job_spec(labels_dict, pod_spec_dict)
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": job_name,
            "labels": labels_dict,
            "annotations": {
                "healthporta.com/queue": spec.queue,
                "healthporta.com/worker-class": spec.worker_class,
                "healthporta.com/importers": ",".join(spec.importers),
                "healthporta.com/run-id": run_id,
            },
        },
        "spec": job_spec_dict,
    }


def worker_job_name(spec: Any, payload: dict[str, Any]) -> str:
    """Return the deterministic Kubernetes Job name for a worker request."""
    run_id = str(payload.get("run_id") or payload.get("import_id") or "adhoc").strip()
    seed = f"{_ENGINE_LABEL}:{spec.worker_class}:{spec.role}:{run_id}"
    suffix = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:10]
    base = _dns_safe(f"hpw-{_ENGINE_LABEL}-{spec.worker_class}-{spec.role}")
    return f"{base[:52]}-{suffix}"[:63].rstrip("-")


def worker_python() -> str:
    """Return the Python executable path used inside Kubernetes worker jobs."""
    return os.getenv("HLTHPRT_WORKER_JOB_PYTHON", "/opt/venv/bin/python")


def _worker_job_container(
    spec: Any,
    worker_request: dict[str, Any],
    image: str,
    volumes_list: list[dict[str, Any]],
) -> dict[str, Any]:
    env_list = [
        {"name": "HLTHPRT_WORKER_LAUNCHER", "value": "process"},
        {"name": "HLTHPRT_IMPORT_NODE_ID", "value": os.getenv("HLTHPRT_IMPORT_NODE_ID", "")},
    ]
    import_id = str(worker_request.get("import_id") or "").strip()
    if import_id:
        env_list.append({"name": "HLTHPRT_IMPORT_ID_OVERRIDE", "value": import_id})

    container_dict: dict[str, Any] = {
        "name": "worker",
        "image": image,
        "imagePullPolicy": os.getenv("HLTHPRT_WORKER_JOB_IMAGE_PULL_POLICY", "IfNotPresent"),
        "workingDir": str(_repo_root()),
        "command": [worker_python(), str(_main_path()), "worker", spec.worker_class, "--burst"],
        "env": env_list,
        "securityContext": _worker_job_container_security_context(),
    }
    env_sources_list = _worker_job_env_from()
    if env_sources_list:
        container_dict["envFrom"] = env_sources_list
    resources_dict = _worker_job_resources()
    if resources_dict:
        container_dict["resources"] = resources_dict
    if volumes_list:
        container_dict["volumeMounts"] = [volume_pair_dict["volumeMount"] for volume_pair_dict in volumes_list]
    return container_dict


def _worker_job_pod_spec(
    container_dict: dict[str, Any],
    volumes_list: list[dict[str, Any]],
) -> dict[str, Any]:
    pod_spec_dict: dict[str, Any] = {
        "restartPolicy": "Never",
        "automountServiceAccountToken": False,
        "securityContext": _worker_job_pod_security_context(has_pvc=bool(volumes_list)),
        "containers": [container_dict],
    }
    if volumes_list:
        pod_spec_dict["volumes"] = [volume_pair_dict["volume"] for volume_pair_dict in volumes_list]
    service_account = os.getenv("HLTHPRT_WORKER_JOB_SERVICE_ACCOUNT", "").strip()
    if service_account:
        pod_spec_dict["serviceAccountName"] = service_account
    pull_secret = os.getenv("HLTHPRT_WORKER_JOB_IMAGE_PULL_SECRET", "").strip()
    if pull_secret:
        pod_spec_dict["imagePullSecrets"] = [{"name": secret_name} for secret_name in _csv(pull_secret)]
    return pod_spec_dict


def _worker_job_labels(spec: Any, run_id: str) -> dict[str, str]:
    labels_dict = {
        "app.kubernetes.io/name": "healthporta-import-worker",
        "app.kubernetes.io/managed-by": "healthporta-worker-launcher",
        "healthporta.com/engine": _ENGINE_LABEL,
        "healthporta.com/worker-class-hash": _label_hash(spec.worker_class),
        "healthporta.com/role": spec.role,
    }
    if run_id:
        labels_dict["healthporta.com/run-id-hash"] = _label_hash(run_id)
    return labels_dict


def _worker_job_spec(labels_dict: dict[str, str], pod_spec_dict: dict[str, Any]) -> dict[str, Any]:
    job_spec_dict: dict[str, Any] = {
        "backoffLimit": int(os.getenv("HLTHPRT_WORKER_JOB_BACKOFF_LIMIT", "0")),
        "ttlSecondsAfterFinished": int(os.getenv("HLTHPRT_WORKER_JOB_TTL_SECONDS", "86400")),
        "template": {
            "metadata": {"labels": labels_dict},
            "spec": pod_spec_dict,
        },
    }
    active_deadline_seconds = int(os.getenv("HLTHPRT_WORKER_JOB_ACTIVE_DEADLINE_SECONDS", "0") or "0")
    if active_deadline_seconds > 0:
        job_spec_dict["activeDeadlineSeconds"] = active_deadline_seconds
    return job_spec_dict


def _worker_job_env_from() -> list[dict[str, Any]]:
    env_sources_list: list[dict[str, Any]] = []
    for name in _csv(os.getenv("HLTHPRT_WORKER_JOB_ENV_FROM_CONFIGMAP", "")):
        env_sources_list.append({"configMapRef": {"name": name}})
    for name in _csv(os.getenv("HLTHPRT_WORKER_JOB_ENV_FROM_SECRET", "")):
        env_sources_list.append({"secretRef": {"name": name}})
    return env_sources_list


def _worker_job_container_security_context() -> dict[str, Any]:
    return {
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]},
    }


def _worker_job_pod_security_context(*, has_pvc: bool) -> dict[str, Any]:
    security_context_dict: dict[str, Any] = {
        "runAsNonRoot": True,
        "runAsUser": 65534,
        "runAsGroup": 65534,
        "seccompProfile": {"type": "RuntimeDefault"},
    }
    if has_pvc:
        security_context_dict["fsGroup"] = 65534
        security_context_dict["fsGroupChangePolicy"] = "OnRootMismatch"
    return security_context_dict


def _worker_job_resources() -> dict[str, Any]:
    resource_requests_dict = {
        key: value
        for key, value in {
            "cpu": os.getenv("HLTHPRT_WORKER_JOB_CPU_REQUEST", "").strip(),
            "memory": os.getenv("HLTHPRT_WORKER_JOB_MEMORY_REQUEST", "").strip(),
        }.items()
        if value
    }
    resource_limits_dict = {
        key: value
        for key, value in {
            "cpu": os.getenv("HLTHPRT_WORKER_JOB_CPU_LIMIT", "").strip(),
            "memory": os.getenv("HLTHPRT_WORKER_JOB_MEMORY_LIMIT", "").strip(),
        }.items()
        if value
    }
    resources_dict: dict[str, Any] = {}
    if resource_requests_dict:
        resources_dict["requests"] = resource_requests_dict
    if resource_limits_dict:
        resources_dict["limits"] = resource_limits_dict
    return resources_dict


def _worker_job_pvc_volumes() -> list[dict[str, Any]]:
    claim_name = os.getenv("HLTHPRT_WORKER_JOB_PVC_NAME", "").strip()
    mount_path = os.getenv("HLTHPRT_WORKER_JOB_PVC_MOUNT_PATH", "").strip()
    if not claim_name or not mount_path:
        return []

    volume_name = os.getenv("HLTHPRT_WORKER_JOB_PVC_VOLUME_NAME", "import-workdir").strip() or "import-workdir"
    return [
        {
            "volume": {
                "name": volume_name,
                "persistentVolumeClaim": {"claimName": claim_name},
            },
            "volumeMount": {
                "name": volume_name,
                "mountPath": mount_path,
            },
        }
    ]


def _label_hash(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:16]


def _dns_safe(value: str) -> str:
    chars = [char.lower() if char.isalnum() else "-" for char in value]
    return "-".join(part for part in "".join(chars).split("-") if part) or "worker"


def _csv(raw: str) -> list[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _main_path() -> Path:
    return _repo_root() / "main.py"
