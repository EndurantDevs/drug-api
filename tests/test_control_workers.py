from __future__ import annotations

from unittest.mock import Mock

import pytest

from api import control_workers
from api.control_imports import NDC_QUEUE_NAME
from process import DrugIndications


def test_worker_registry_exposes_drug_workers():
    items = control_workers.worker_registry()
    by_importer = {importer: item for item in items for importer in item["importers"]}

    assert by_importer["ndc"]["queue"] == NDC_QUEUE_NAME
    assert by_importer["label"]["worker_class"] == "process.Labeling"
    assert by_importer["drug-indications"]["worker_class"] == "process.DrugIndications"


def test_drug_indications_worker_has_production_timeout():
    assert DrugIndications.job_timeout == 86400


def test_ensure_worker_starts_registered_burst_worker(monkeypatch, tmp_path):
    captured_process_dict: dict[str, object] = {}

    class FakeProcess:
        pid = 9876

    def fake_popen(cmd, *, cwd, env, stdout, stderr, start_new_session):
        captured_process_dict.update(
            {
                "cmd": cmd,
                "cwd": cwd,
                "env": env,
                "stdout": stdout,
                "stderr": stderr,
                "start_new_session": start_new_session,
            }
        )
        return FakeProcess()

    monkeypatch.setenv("HLTHPRT_WORKER_STATE_DIR", str(tmp_path / "state"))
    monkeypatch.setenv("HLTHPRT_WORKER_LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.setattr(control_workers.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(control_workers, "_pid_running", lambda pid: pid == FakeProcess.pid)
    monkeypatch.setattr(control_workers, "_pid_matches_current_node", lambda pid: True)

    ensure_result = control_workers.ensure_worker({"importer": "drug-indications", "run_id": "run_1"})

    assert ensure_result["status"] == "started"
    assert ensure_result["items"][0]["worker_class"] == "process.DrugIndications"
    assert captured_process_dict["cmd"][-2:] == ["process.DrugIndications", "--burst"]
    assert captured_process_dict["start_new_session"] is True


def test_ensure_worker_can_create_kubernetes_job(monkeypatch):
    calls: list[tuple[str, str, dict[str, object] | None]] = []

    def fake_request(method, path, body=None):
        calls.append((method, path, body))
        if method == "GET" and any(recorded_call[0] == "POST" for recorded_call in calls):
            return {
                "items": [
                    {
                        "metadata": {"name": "worker-job"},
                        "status": {"active": 1},
                    }
                ]
            }
        return {"items": []}

    monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "kubernetes")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", "ghcr.io/example/drug-api:test")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_ENV_FROM_CONFIGMAP", "drug-api-config")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_ENV_FROM_SECRET", "drug-api-secret")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_PVC_NAME", "import-workdir")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_PVC_MOUNT_PATH", "/work")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_SERVICE_ACCOUNT", "drug-api-worker")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE_PULL_SECRET", "ghcr-pull,mirror-pull")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_CPU_REQUEST", "100m")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_MEMORY_LIMIT", "256Mi")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_ACTIVE_DEADLINE_SECONDS", "43200")
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "local_drug")
    monkeypatch.setattr(control_workers, "_kubernetes_configured", lambda: True)
    monkeypatch.setattr(control_workers, "_kubernetes_namespace", lambda: "healthporta-dev")
    monkeypatch.setattr(control_workers, "_kubernetes_request", fake_request)

    ensure_result = control_workers.ensure_worker({"importer": "ndc", "import_id": "import_123", "run_id": "run_123"})

    assert ensure_result["status"] == "started"
    post = next(call for call in calls if call[0] == "POST")
    job = post[2]
    assert post[1] == "/apis/batch/v1/namespaces/healthporta-dev/jobs"
    assert job["kind"] == "Job"
    container = job["spec"]["template"]["spec"]["containers"][0]
    assert container["image"] == "ghcr.io/example/drug-api:test"
    assert container["command"][-2:] == ["process.NDC", "--burst"]
    assert {"name": "HLTHPRT_IMPORT_ID_OVERRIDE", "value": "import_123"} in container["env"]
    assert {"configMapRef": {"name": "drug-api-config"}} in container["envFrom"]
    assert {"secretRef": {"name": "drug-api-secret"}} in container["envFrom"]
    assert container["resources"] == {
        "requests": {"cpu": "100m"},
        "limits": {"memory": "256Mi"},
    }
    assert container["volumeMounts"] == [{"name": "import-workdir", "mountPath": "/work"}]
    pod_spec = job["spec"]["template"]["spec"]
    assert pod_spec["serviceAccountName"] == "drug-api-worker"
    assert pod_spec["imagePullSecrets"] == [{"name": "ghcr-pull"}, {"name": "mirror-pull"}]
    assert pod_spec["volumes"] == [{"name": "import-workdir", "persistentVolumeClaim": {"claimName": "import-workdir"}}]
    assert job["spec"]["activeDeadlineSeconds"] == 43200


def test_kubernetes_completed_worker_job_is_recreated(monkeypatch):
    calls: list[tuple[str, str, dict[str, object] | None]] = []

    def fake_request(method, path, body=None):
        calls.append((method, path, body))
        if method == "GET":
            if any(recorded_call[0] == "POST" for recorded_call in calls):
                return {"items": [{"metadata": {"name": "worker-job"}, "status": {"active": 1}}]}
            return {"items": [{"metadata": {"name": "worker-job"}, "status": {"succeeded": 1}}]}
        return {}

    monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "kubernetes")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", "ghcr.io/example/drug-api:test")
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "local_drug")
    monkeypatch.setattr(control_workers, "_kubernetes_configured", lambda: True)
    monkeypatch.setattr(control_workers, "_kubernetes_namespace", lambda: "healthporta-dev")
    monkeypatch.setattr(control_workers, "_kubernetes_request", fake_request)

    ensure_result = control_workers.ensure_worker({"importer": "drug-indications", "run_id": "run_123"})

    assert ensure_result["status"] == "started"
    assert any(call[0] == "DELETE" and call[1].endswith("/jobs/worker-job") for call in calls)
    assert any(call[0] == "POST" and call[1] == "/apis/batch/v1/namespaces/healthporta-dev/jobs" for call in calls)


@pytest.mark.parametrize("status,condition", [("failed", "Failed"), ("succeeded", "Complete")])
def test_terminal_label_job_is_retained_for_recovery(monkeypatch, status, condition):
    spec = control_workers._BY_IMPORTER["label"]
    payload = {"importer": "label", "run_id": "original-run"}
    job_dict = control_workers.worker_job_manifest(spec, payload, "example")
    job_dict["metadata"]["uid"] = "original-worker-uid"
    job_dict["status"] = {status: 1, "conditions": [{"type": condition, "status": "True"}]}
    requests = Mock(return_value={"items": [job_dict]})
    monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "kubernetes")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", "example")
    monkeypatch.setattr(control_workers, "_kubernetes_configured", lambda: True)
    monkeypatch.setattr(control_workers, "_kubernetes_namespace", lambda: "test-workers")
    monkeypatch.setattr(control_workers, "_kubernetes_request", requests)

    ensured_dict = control_workers.ensure_worker(payload)

    assert ensured_dict["status"] == "exists"
    assert ensured_dict["items"][0]["job_uid"] == "original-worker-uid"
    assert "retained for recovery" in ensured_dict["items"][0]["message"]
    assert all(call.args[0] == "GET" for call in requests.call_args_list)


def test_distinct_label_worker_preserves_terminal_predecessor(monkeypatch):
    spec = control_workers._BY_IMPORTER["label"]
    payload = {"run_id": "new-run"}
    predecessor_dict = control_workers.worker_job_manifest(spec, {"run_id": "original-run"}, "example")
    predecessor_dict["metadata"]["uid"] = "original-worker-uid"
    predecessor_dict["status"] = {"failed": 1, "conditions": [{"type": "Failed", "status": "True"}]}
    state_dict = control_workers._worker_jobs_state([predecessor_dict], control_workers.worker_job_name(spec, payload))
    requests = Mock(return_value={"items": []})
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", "example/worker@sha256:" + "1" * 64)
    monkeypatch.setattr(control_workers, "_kubernetes_namespace", lambda: "test-workers")
    monkeypatch.setattr(control_workers, "_kubernetes_request", requests)
    monkeypatch.setattr(control_workers, "_worker_state", Mock(return_value={"running": True}))

    ensured_dict = control_workers._ensure_kubernetes_job(spec, payload, state_dict)

    assert ensured_dict["status"] == "started"
    requests.assert_called_once()
    assert requests.call_args.args[0] == "POST"
    assert requests.call_args.args[2]["metadata"]["name"] != predecessor_dict["metadata"]["name"]
    assert predecessor_dict["metadata"]["uid"] == "original-worker-uid"


def test_find_running_pid_ignores_other_node_worker(monkeypatch):
    output = """
111 /opt/python main.py worker process.NDC HLTHPRT_IMPORT_NODE_ID=other_drug
222 /opt/python main.py worker process.NDC HLTHPRT_IMPORT_NODE_ID=local_drug
"""

    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "local_drug")
    monkeypatch.setattr(control_workers.subprocess, "check_output", lambda *_args, **_kwargs: output)

    spec = control_workers._BY_QUEUE[NDC_QUEUE_NAME]

    assert control_workers._find_running_pid(spec) == 222


@pytest.mark.parametrize(
    "condition,expected", [("Complete", "succeeded"), ("Failed", "failed"), ("FailureTarget", None), (None, None)]
)
def test_worker_proof_uses_actual_job_identity(monkeypatch, condition, expected):
    image = "ghcr.io/example/worker@sha256:" + "1" * 64
    job_dict = {
        "metadata": {
            "name": "observed-job",
            "uid": "observed-uid",
            "annotations": {"healthporta.com/run-id": "observed-run"},
        },
        "spec": {"template": {"spec": {"containers": [{"name": "worker", "image": image}]}}},
        "status": {
            "active": 0,
            "failed": 1,
            "conditions": [] if condition is None else [{"type": condition, "status": "True"}],
        },
    }
    monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "kubernetes")
    monkeypatch.setattr(control_workers, "_kubernetes_configured", lambda: True)
    monkeypatch.setattr(control_workers, "_kubernetes_namespace", lambda: "example-workers")
    monkeypatch.setattr(control_workers, "_kubernetes_request", Mock(return_value={"items": [job_dict]}))
    state_dict = control_workers._worker_state(control_workers._BY_IMPORTER["label"], {"run_id": "requested-run"})
    assert state_dict["run_id"] == "observed-run"
    assert state_dict["job_uid"] == "observed-uid"
    assert state_dict["terminal_status"] == expected
    assert state_dict["worker_image"] == image and state_dict["active_jobs"] == 0


@pytest.mark.parametrize("importer", ["ndc", "label", "drug-indications"])
def test_only_label_gets_runtime_identity_fields(importer):
    job = control_workers.worker_job_manifest(control_workers._BY_IMPORTER[importer], {"run_id": "example"}, "example")
    env_by_name = {value["name"]: value for value in job["spec"]["template"]["spec"]["containers"][0]["env"]}
    names = {"HLTHPRT_LABEL_WORKER_JOB_NAME", "HLTHPRT_LABEL_WORKER_JOB_UID", "HLTHPRT_WORKER_JOB_IMAGE"}
    if importer == "label":
        assert env_by_name["HLTHPRT_WORKER_JOB_IMAGE"] == {"name": "HLTHPRT_WORKER_JOB_IMAGE", "value": "example"}
        assert (
            env_by_name["HLTHPRT_LABEL_WORKER_JOB_NAME"]["valueFrom"]["fieldRef"]["fieldPath"]
            == "metadata.labels['batch.kubernetes.io/job-name']"
        )
        assert (
            env_by_name["HLTHPRT_LABEL_WORKER_JOB_UID"]["valueFrom"]["fieldRef"]["fieldPath"]
            == "metadata.labels['batch.kubernetes.io/controller-uid']"
        )
    else:
        assert not names.intersection(env_by_name)


@pytest.mark.parametrize("damage", [None, "name", "scope"])
def test_claimed_job_lookup_checks_actual_scope(monkeypatch, damage):
    spec = control_workers._BY_IMPORTER["label"]
    job = control_workers.worker_job_manifest(spec, {"run_id": "other-launch-run"}, "example")
    job["metadata"].update(name="actual-job", uid="actual-uid")
    job["status"] = {"succeeded": 1, "conditions": [{"type": "Complete", "status": "True"}]}
    if damage == "name":
        job["metadata"]["name"] = "other-job"
    if damage == "scope":
        job["metadata"]["labels"]["healthporta.com/engine"] = "other"
    request = Mock(return_value=job)
    monkeypatch.setattr(control_workers, "_kubernetes_configured", lambda: True)
    monkeypatch.setattr(control_workers, "_kubernetes_namespace", lambda: "example-workers")
    monkeypatch.setattr(control_workers, "_kubernetes_request", request)
    state_dict = control_workers._kubernetes_worker_state(spec, claimed_job_name="actual-job")
    assert request.call_args.args == ("GET", "/apis/batch/v1/namespaces/example-workers/jobs/actual-job")
    assert state_dict["job_uid"] == (None if damage else "actual-uid")
    assert state_dict["terminal_status"] == (None if damage else "succeeded")
