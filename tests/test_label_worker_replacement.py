"""Queued startup repair preserves terminal consumer evidence and import ownership."""

import asyncio
import copy
import http.client
import importlib
import json
import threading
from contextlib import asynccontextmanager, contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from urllib.parse import parse_qs, urlsplit

import pytest

from api import control_workers
from process.label_publish import label_publication_protocol

control = importlib.import_module("api.control")


def _worker_transport(jobs_dict, calls_list):
    """Simulate only the Kubernetes Job transport used by the actual launcher."""

    def request(method, path, body=None):
        calls_list.append((method, path, body))
        if method == "DELETE":
            pytest.fail("Label startup repair deleted retained worker evidence")
        if method == "POST":
            name = body["metadata"]["name"]
            if name in jobs_dict:
                raise control_workers._KubernetesApiError(409, "already exists")
            body["metadata"].update(
                uid=f"worker-uid-{len(jobs_dict)}", creationTimestamp=f"2026-01-01T00:00:0{len(jobs_dict)}Z"
            )
            jobs_dict[name] = body
            return body
        if "/jobs/" in path:
            name = path.rsplit("/", 1)[1]
            if name not in jobs_dict:
                raise control_workers._KubernetesApiError(404, "not found")
            return jobs_dict[name]
        selector = parse_qs(urlsplit(path).query)["labelSelector"][0]
        labels_dict = dict(pair.split("=", 1) for pair in selector.split(","))
        return {
            "items": [
                job
                for job in reversed(list(jobs_dict.values()))
                if all(job["metadata"]["labels"].get(key) == value for key, value in labels_dict.items())
            ]
        }

    return request


@pytest.fixture
def replacement_case(monkeypatch):
    image = "ghcr.io/example/worker@sha256:" + "1" * 64
    monkeypatch.setenv("DB_SCHEMA", "sample")
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "example-node")
    monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "kubernetes")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", image)
    monkeypatch.setattr(control_workers, "_kubernetes_configured", lambda: True)
    monkeypatch.setattr(control_workers, "_kubernetes_namespace", lambda: "test-workers")
    monkeypatch.setattr(control, "_require_control_auth", lambda _request: None)
    run_id = "queued-run"
    spec = control_workers._BY_IMPORTER["label"]
    job_dict = control_workers.worker_job_manifest(spec, {"run_id": run_id}, image)
    job_dict["metadata"].update(uid="original-worker-uid", creationTimestamp="2026-01-01T00:00:00Z")
    job_dict["status"] = {"failed": 1, "conditions": [{"type": "Failed", "status": "True"}]}
    jobs_dict = {job_dict["metadata"]["name"]: job_dict}
    calls_list = []
    http_request = control_workers._kubernetes_request
    monkeypatch.setattr(control_workers, "_kubernetes_request", _worker_transport(jobs_dict, calls_list))
    run_row_dict = {
        "run_id": run_id,
        "status": "queued",
        "started_at": None,
        "finished_at": None,
        "error": None,
        "progress": {"done": 0, "pct": 0},
        "node_id": "example-node",
        "metrics": {"label_publication_protocol": label_publication_protocol(run_id, "sample", "example-node")},
    }
    connection = SimpleNamespace(first=AsyncMock(return_value=run_row_dict), status=AsyncMock())

    @asynccontextmanager
    async def acquire():
        yield connection

    monkeypatch.setattr(control_workers, "db", SimpleNamespace(acquire=acquire))
    return SimpleNamespace(
        spec=spec,
        run_id=run_id,
        job=job_dict,
        jobs_dict=jobs_dict,
        calls_list=calls_list,
        run_row_dict=run_row_dict,
        connection=connection,
        http_request=http_request,
    )


async def _ensure_route(run_id):
    result = await control.control_ensure_worker(SimpleNamespace(json={"importer": "label", "run_id": run_id}))
    assert result.status == 202
    return json.loads(result.body)


@pytest.mark.asyncio
async def test_route_replaces_pre_dequeue_startup_failure(replacement_case):
    case = replacement_case
    before_dict = copy.deepcopy(case.run_row_dict)

    result_dict = await _ensure_route(case.run_id)
    repeated_dict = await _ensure_route(case.run_id)

    assert result_dict["status"] == "started" and repeated_dict["status"] == "exists"
    posts_list = [call for call in case.calls_list if call[0] == "POST"]
    assert len(posts_list) == 1
    replacement_dict = posts_list[0][2]
    expected_name = control_workers.worker_job_name(
        case.spec, {"run_id": case.run_id}, previous_job_uid="original-worker-uid"
    )
    assert replacement_dict["metadata"]["name"] == expected_name != case.job["metadata"]["name"]
    assert replacement_dict["metadata"]["annotations"]["healthporta.com/run-id"] == case.run_id
    container_dict = replacement_dict["spec"]["template"]["spec"]["containers"][0]
    assert {"name": "HLTHPRT_WORKER_JOB_IMAGE", "value": container_dict["image"]} in container_dict["env"]
    assert case.job["metadata"]["uid"] == "original-worker-uid" and case.run_row_dict == before_dict
    assert "FOR UPDATE" in str(case.connection.first.call_args.args[0])
    assert all("SET LOCAL" in call.args[0] for call in case.connection.status.call_args_list)


@pytest.mark.asyncio
async def test_repeated_startup_failure_uses_newest_job(replacement_case):
    case = replacement_case
    case.job["metadata"]["uid"] = "worker-uid-0"
    await _ensure_route(case.run_id)
    replacement_dict = list(case.jobs_dict.values())[-1]
    replacement_dict["metadata"]["creationTimestamp"] = case.job["metadata"]["creationTimestamp"]
    assert replacement_dict["metadata"]["name"] < case.job["metadata"]["name"]
    replacement_dict["status"] = {"failed": 1, "conditions": [{"type": "Failed", "status": "True"}]}

    result_dict = await _ensure_route(case.run_id)

    assert result_dict["status"] == "started" and len(case.jobs_dict) == 3
    newest_dict = list(case.jobs_dict.values())[-1]
    assert newest_dict["metadata"]["name"] == control_workers.worker_job_name(
        case.spec, {"run_id": case.run_id}, previous_job_uid=replacement_dict["metadata"]["uid"]
    )
    assert len({job["metadata"]["uid"] for job in case.jobs_dict.values()}) == 3


@pytest.mark.parametrize(
    "importer,payload,expected",
    [
        ("ndc", {"run_id": "example-run"}, "hpw-drug-process-ndc-start-ae11bb9f8f"),
        ("ndc", {"import_id": "existing-id"}, "hpw-drug-process-ndc-start-677eee259c"),
        ("ndc", {}, "hpw-drug-process-ndc-start-9f43386bf9"),
        ("ndc", {"run_id": "label-" + "x" * 123}, "hpw-drug-process-ndc-start-9f518fb220"),
        ("label", {"run_id": "example-run"}, "hpw-drug-process-labeling-start-b6ff598049"),
        ("label", {"import_id": "existing-id"}, "hpw-drug-process-labeling-start-25f62d790f"),
        ("label", {}, "hpw-drug-process-labeling-start-c23270e171"),
        ("label", {"run_id": "label-" + "x" * 123}, "hpw-drug-process-labeling-start-d69abfda19"),
        ("drug-indications", {"run_id": "example-run"}, "hpw-drug-process-drugindications-start-51df794b64"),
        ("drug-indications", {"import_id": "existing-id"}, "hpw-drug-process-drugindications-start-9e21434930"),
        ("drug-indications", {}, "hpw-drug-process-drugindications-start-e0f55a02cc"),
        ("drug-indications", {"run_id": "label-" + "x" * 123}, "hpw-drug-process-drugindications-start-b4efa122aa"),
    ],
)
def test_existing_worker_names_preserve_exact_identity(importer, payload, expected):
    assert control_workers.worker_job_name(control_workers._BY_IMPORTER[importer], payload) == expected


def test_replacement_uid_uses_sha256_only(monkeypatch):
    monkeypatch.setattr(control_workers.hashlib, "sha1", Mock(side_effect=AssertionError("replacement reached SHA1")))
    name = control_workers.worker_job_name(
        control_workers._BY_IMPORTER["label"], {"run_id": "queued-run"}, previous_job_uid="worker-uid-0"
    )
    assert name == "hpw-drug-process-labeling-start-0d4afb4914"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage",
    [
        "running",
        "failed",
        "succeeded",
        "canceled",
        "canceling",
        "claim",
        "intent",
        "publication",
        "protocol",
        "node",
        "missing",
        "started_at",
        "finished_at",
        "error",
        "done",
        "pct",
    ],
)
async def test_startup_refuses_owned_or_ambiguous_runs(replacement_case, damage):
    case = replacement_case
    metrics_dict = case.run_row_dict["metrics"]
    updates_dict = {
        "running": {"status": "running"},
        "failed": {"status": "failed"},
        "succeeded": {"status": "succeeded"},
        "canceled": {"status": "canceled"},
        "canceling": {"status": "canceling"},
        "claim": {"metrics": {**metrics_dict, "label_worker_claim": None}},
        "intent": {"metrics": {**metrics_dict, "label_publication_intent": None}},
        "publication": {"metrics": {**metrics_dict, "label_publication": None}},
        "protocol": {"metrics": {}},
        "node": {"node_id": "foreign-node"},
        "started_at": {"started_at": "retained"},
        "finished_at": {"finished_at": "retained"},
        "error": {"error": "retained"},
        "done": {"progress": {"done": 1}},
        "pct": {"progress": {"pct": 1}},
    }
    if damage == "missing":
        case.connection.first.return_value = None
    else:
        case.run_row_dict.update(updates_dict[damage])
    before_dict = copy.deepcopy(case.run_row_dict)

    result_dict = await _ensure_route(case.run_id)

    assert result_dict["status"] == "failed"
    assert "unclaimed queued admission" in result_dict["items"][0]["message"]
    assert len(case.jobs_dict) == 1 and all(call[0] == "GET" for call in case.calls_list)
    assert case.run_row_dict == before_dict


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["uid", "name", "terminal", "active", "pending"])
async def test_startup_refuses_ambiguous_job_evidence(replacement_case, monkeypatch, damage):
    case = replacement_case
    state_dict = control_workers._worker_state(case.spec, {"run_id": case.run_id})
    if damage == "uid":
        state_dict["job_uid"] = None
    elif damage == "name":
        state_dict["job_name"] = None
    elif damage == "terminal":
        state_dict["terminal_status"] = None
    elif damage == "active":
        state_dict.update(running=True, active_jobs=1)
    else:
        state_dict["job_status"] = "pending"
    monkeypatch.setattr(control_workers, "_worker_state", Mock(return_value=state_dict))

    result_dict = await _ensure_route(case.run_id)

    assert result_dict["status"] == {"active": "already_running", "pending": "exists"}.get(damage, "failed")
    assert len(case.jobs_dict) == 1 and all(call[0] == "GET" for call in case.calls_list)


@pytest.mark.asyncio
async def test_replacement_conflict_preserves_original_job(replacement_case):
    case = replacement_case
    state_dict = control_workers._worker_state(case.spec, {"run_id": case.run_id})
    request_dict = {"importer": "label", "run_id": case.run_id}
    first_dict = control_workers._ensure_kubernetes_job(
        case.spec, request_dict, state_dict, previous_job_uid="original-worker-uid"
    )
    repeated_dict = control_workers._ensure_kubernetes_job(
        case.spec, request_dict, state_dict, previous_job_uid="original-worker-uid"
    )
    assert first_dict["status"] == "started" and repeated_dict["status"] == "exists"
    assert len(case.jobs_dict) == 2 and case.job["metadata"]["uid"] == "original-worker-uid"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "importer,launcher",
    [("ndc", "kubernetes"), ("drug-indications", "kubernetes"), ("label", "process"), ("unknown", "kubernetes")],
)
async def test_other_ensure_modes_keep_existing_entrypoint(monkeypatch, importer, launcher):
    monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", launcher)
    expected_dict = {"status": "existing", "items": []}
    ensure = Mock(return_value=expected_dict)
    monkeypatch.setattr(control_workers, "ensure_worker", ensure)
    assert await control_workers.ensure_worker_request({"importer": importer}) is expected_dict
    ensure.assert_called_once_with({"importer": importer})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "image", ["example/worker:latest", "example/worker@sha256:short", "example/worker@sha256:" + "A" * 64]
)
async def test_label_refuses_mutable_image_before_creating_job(replacement_case, monkeypatch, image):
    case = replacement_case
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", image)

    result_dict = await _ensure_route(case.run_id)

    assert result_dict["status"] == "failed"
    assert "immutable SHA-256 image" in result_dict["items"][0]["message"]
    assert len(case.jobs_dict) == 1 and all(method == "GET" for method, *_rest in case.calls_list)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcome", ["created", "active", "missing", "foreign-image", "foreign-run", "foreign-scope", "missing-uid"]
)
@pytest.mark.parametrize("error_status", [0, 408, 500])
async def test_uncertain_create_reads_exact_deterministic_job(replacement_case, monkeypatch, outcome, error_status):
    case = replacement_case
    request = control_workers._kubernetes_request

    def uncertain_request(method, path, body=None):
        if method != "POST":
            return request(method, path, body)
        if outcome == "missing":
            case.calls_list.append((method, path, body))
            raise control_workers._KubernetesApiError(error_status, "create response lost")
        body = copy.deepcopy(body)
        request(method, path, body)
        if outcome == "active":
            body["status"] = {"active": 1}
        if outcome == "foreign-image":
            body["spec"]["template"]["spec"]["containers"][0]["image"] = "foreign:latest"
        if outcome == "foreign-run":
            body["metadata"]["annotations"]["healthporta.com/run-id"] = "other-run"
        if outcome == "foreign-scope":
            body["metadata"]["labels"]["healthporta.com/engine"] = "foreign"
        if outcome == "missing-uid":
            body["metadata"].pop("uid")
        raise control_workers._KubernetesApiError(error_status, "create response lost")

    monkeypatch.setattr(control_workers, "_kubernetes_request", uncertain_request)
    result_dict = await _ensure_route(case.run_id)

    assert result_dict["status"] == {"created": "exists", "active": "already_running"}.get(outcome, "failed")
    assert sum(method == "POST" for method, *_rest in case.calls_list) == 1
    expected_name = control_workers.worker_job_name(
        case.spec, {"run_id": case.run_id}, previous_job_uid="original-worker-uid"
    )
    assert case.calls_list[-1][0:2] == ("GET", f"/apis/batch/v1/namespaces/test-workers/jobs/{expected_name}")
    assert case.job["metadata"]["uid"] == "original-worker-uid"


@pytest.mark.asyncio
@pytest.mark.parametrize("cancellation", ["parent", "timeout"])
async def test_cancellation_keeps_admission_reserved_until_create_settles(replacement_case, monkeypatch, cancellation):
    case = replacement_case
    started, release = threading.Event(), threading.Event()
    reservation_states_list = []
    request = control_workers._kubernetes_request

    @asynccontextmanager
    async def acquire():
        reservation_states_list.append(True)
        try:
            yield case.connection
        finally:
            reservation_states_list.append(False)

    def delayed_request(method, path, body=None):
        if method == "POST":
            started.set()
            if not release.wait(3):
                raise RuntimeError("test did not release bounded create")
            assert reservation_states_list == [True]
            request(method, path, body)
            raise control_workers._KubernetesApiError(0, "create response lost")
        return request(method, path, body)

    monkeypatch.setattr(control_workers, "db", SimpleNamespace(acquire=acquire))
    monkeypatch.setattr(control_workers, "_kubernetes_request", delayed_request)

    async def bounded_route():
        async with asyncio.timeout(0.03 if cancellation == "timeout" else None):
            return await _ensure_route(case.run_id)

    worker = asyncio.create_task(bounded_route())
    try:
        assert await asyncio.to_thread(started.wait, 3)
        if cancellation == "timeout":
            await asyncio.sleep(0.06)
        else:
            for _ in range(2):
                worker.cancel()
                await asyncio.sleep(0)
        assert not worker.done() and reservation_states_list == [True]
        release.set()
        with pytest.raises(TimeoutError if cancellation == "timeout" else asyncio.CancelledError):
            await worker
        assert reservation_states_list == [True, False] and len(case.jobs_dict) == 2
        assert (await _ensure_route(case.run_id))["status"] == "exists"
        assert sum(method == "POST" for method, *_rest in case.calls_list) == 1
    finally:
        release.set()
        await asyncio.gather(worker, return_exceptions=True)


@pytest.mark.parametrize("raw", [None, b"invalid", b"\xff", b"[]", "unreadable-http-error"])
def test_transport_classifies_timeout_and_unreadable_response_as_uncertain(monkeypatch, tmp_path, raw):
    token = tmp_path / "token"
    token.write_text("synthetic")
    monkeypatch.setattr(control_workers, "_K8S_API_TOKEN", token)
    monkeypatch.setattr(control_workers, "_K8S_API_CA", tmp_path / "absent-ca")
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "127.0.0.1")

    @contextmanager
    def response(*_args, **_kwargs):
        if raw is None:
            raise TimeoutError("response timed out")
        if raw == "unreadable-http-error":
            with control_workers.urllib.error.HTTPError(
                "https://example.invalid", 503, "unavailable", {}, None
            ) as error:
                error.read = Mock(side_effect=TimeoutError("error response timed out"))
                raise error
        yield SimpleNamespace(read=lambda: raw)

    monkeypatch.setattr(control_workers.urllib.request, "urlopen", response)
    with pytest.raises(control_workers._KubernetesApiError) as caught:
        control_workers._kubernetes_request("POST", "/example/jobs", {})
    assert caught.value.status == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["response-body", "error-body", "status-line"])
async def test_http_protocol_failure_reconciles_created_label_job(replacement_case, monkeypatch, tmp_path, failure):
    token = tmp_path / "token"
    token.write_text("synthetic")
    monkeypatch.setattr(control_workers, "_K8S_API_TOKEN", token)
    monkeypatch.setattr(control_workers, "_K8S_API_CA", tmp_path / "absent-ca")
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "127.0.0.1")
    monkeypatch.setattr(control_workers, "_kubernetes_request", replacement_case.http_request)
    transport = _worker_transport(replacement_case.jobs_dict, replacement_case.calls_list)
    reservation = SimpleNamespace(active=False)
    expected_name = control_workers.worker_job_name(
        replacement_case.spec, {"run_id": replacement_case.run_id}, previous_job_uid="original-worker-uid"
    )

    @asynccontextmanager
    async def acquire():
        reservation.active = True
        try:
            yield replacement_case.connection
        finally:
            reservation.active = False

    @contextmanager
    def response(read):
        yield SimpleNamespace(read=read)

    def urlopen(request, **_kwargs):
        method = request.get_method()
        if method == "POST" or request.selector.endswith("/" + expected_name):
            assert reservation.active
        body = json.loads(request.data) if request.data is not None else None
        observed = transport(method, request.selector, body)
        if method == "POST":
            if failure == "status-line":
                raise http.client.BadStatusLine("incomplete status")
            interrupted = http.client.IncompleteRead(b"incomplete", 8)
            if failure == "error-body":
                with control_workers.urllib.error.HTTPError(request.full_url, 503, "unavailable", {}, None) as error:
                    error.read = Mock(side_effect=interrupted)
                    raise error
            return response(Mock(side_effect=interrupted))
        return response(Mock(return_value=json.dumps(observed).encode()))

    monkeypatch.setattr(control_workers, "db", SimpleNamespace(acquire=acquire))
    monkeypatch.setattr(control_workers.urllib.request, "urlopen", urlopen)
    result_dict = await _ensure_route(replacement_case.run_id)

    assert result_dict["status"] == "exists"
    assert sum(method == "POST" for method, *_rest in replacement_case.calls_list) == 1
    assert replacement_case.calls_list[-1][0:2] == (
        "GET",
        f"/apis/batch/v1/namespaces/test-workers/jobs/{expected_name}",
    )
    assert len(replacement_case.jobs_dict) == 2 and replacement_case.job["metadata"]["uid"] == "original-worker-uid"
    assert "FOR UPDATE" in str(replacement_case.connection.first.call_args.args[0])
    assert not reservation.active


@pytest.mark.parametrize("status", [403, 409, 503])
def test_readable_http_error_preserves_explicit_status(monkeypatch, tmp_path, status):
    token = tmp_path / "token"
    token.write_text("synthetic")
    monkeypatch.setattr(control_workers, "_K8S_API_TOKEN", token)
    monkeypatch.setattr(control_workers, "_K8S_API_CA", tmp_path / "absent-ca")
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "127.0.0.1")
    with control_workers.urllib.error.HTTPError("https://example.invalid", status, "rejected", {}, None) as error:
        error.read = Mock(return_value=b"explicit rejection")
        monkeypatch.setattr(control_workers.urllib.request, "urlopen", Mock(side_effect=error))

        with pytest.raises(control_workers._KubernetesApiError) as caught:
            control_workers._kubernetes_request("POST", "/example/jobs", {})

    assert caught.value.status == status
    assert str(caught.value) == "explicit rejection"
