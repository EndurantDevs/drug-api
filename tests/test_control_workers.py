from __future__ import annotations

from api import control_workers
from api.control_imports import NDC_QUEUE_NAME


def test_worker_registry_exposes_drug_workers():
    items = control_workers.worker_registry()
    by_importer = {importer: item for item in items for importer in item["importers"]}

    assert by_importer["ndc"]["queue"] == NDC_QUEUE_NAME
    assert by_importer["label"]["worker_class"] == "process.Labeling"
    assert by_importer["drug-indications"]["worker_class"] == "process.DrugIndications"


def test_ensure_worker_starts_registered_burst_worker(monkeypatch, tmp_path):
    captured: dict[str, object] = {}

    class FakeProcess:
        pid = 9876

    def fake_popen(cmd, *, cwd, env, stdout, stderr, start_new_session):
        captured.update(
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

    result = control_workers.ensure_worker({"importer": "drug-indications", "run_id": "run_1"})

    assert result["status"] == "started"
    assert result["items"][0]["worker_class"] == "process.DrugIndications"
    assert captured["cmd"][-2:] == ["process.DrugIndications", "--burst"]
    assert captured["start_new_session"] is True


def test_ensure_worker_can_create_kubernetes_job(monkeypatch):
    calls: list[tuple[str, str, dict[str, object] | None]] = []

    def fake_request(method, path, body=None):
        calls.append((method, path, body))
        if method == "GET" and any(item[0] == "POST" for item in calls):
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
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", "ghcr.io/endurantdevs/drug-api:dev")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_ENV_FROM_CONFIGMAP", "drug-api-config")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_ENV_FROM_SECRET", "drug-api-secret")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_ACTIVE_DEADLINE_SECONDS", "43200")
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "local_drug")
    monkeypatch.setattr(control_workers, "_kubernetes_configured", lambda: True)
    monkeypatch.setattr(control_workers, "_kubernetes_namespace", lambda: "healthporta-dev")
    monkeypatch.setattr(control_workers, "_kubernetes_request", fake_request)

    result = control_workers.ensure_worker({"importer": "ndc", "run_id": "run_123"})

    assert result["status"] == "started"
    post = next(call for call in calls if call[0] == "POST")
    job = post[2]
    assert post[1] == "/apis/batch/v1/namespaces/healthporta-dev/jobs"
    assert job["kind"] == "Job"
    container = job["spec"]["template"]["spec"]["containers"][0]
    assert container["image"] == "ghcr.io/endurantdevs/drug-api:dev"
    assert container["command"][-2:] == ["process.NDC", "--burst"]
    assert {"configMapRef": {"name": "drug-api-config"}} in container["envFrom"]
    assert {"secretRef": {"name": "drug-api-secret"}} in container["envFrom"]
    assert job["spec"]["activeDeadlineSeconds"] == 43200


def test_find_running_pid_ignores_other_node_worker(monkeypatch):
    output = """
111 /opt/python main.py worker process.NDC HLTHPRT_IMPORT_NODE_ID=other_drug
222 /opt/python main.py worker process.NDC HLTHPRT_IMPORT_NODE_ID=local_drug
"""

    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "local_drug")
    monkeypatch.setattr(control_workers.subprocess, "check_output", lambda *_args, **_kwargs: output)

    spec = control_workers._BY_QUEUE[NDC_QUEUE_NAME]  # pylint: disable=protected-access

    assert control_workers._find_running_pid(spec) == 222  # pylint: disable=protected-access
