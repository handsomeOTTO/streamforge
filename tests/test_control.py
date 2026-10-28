# SPDX-FileCopyrightText: 2026 StreamForge contributors
# SPDX-License-Identifier: Apache-2.0

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeout

import pytest
from fastapi.testclient import TestClient

from streamforge.application.api import create_app
from streamforge.application.controller import ServiceController
from streamforge.messages import ResultMessage
from tests.test_service import enqueue

pytestmark = pytest.mark.redis


class PausingStatusController(ServiceController):
    reader_id = None
    paused = False

    def __getattribute__(self, name):
        value = super().__getattribute__(name)
        if name == "_service" and threading.get_ident() == self.reader_id and not self.paused:
            self.paused = True
            self.entered.set()
            if not self.release.wait(5):
                raise AssertionError("Status synchronization timed out")
        return value


def wait_for_output(client, key):
    deadline = time.monotonic() + 10
    while not client.llen(key):
        if time.monotonic() >= deadline:
            raise AssertionError("Redis workflow did not publish a result")
        time.sleep(0.01)
    return ResultMessage.model_validate_json(client.lpop(key))


def test_control_api_start_stop_reconfigure_and_version_preserve_workflow(redis_setup):
    config, redis_client, prefix = redis_setup
    app = create_app(config, autostart=False)
    with TestClient(app) as client:
        enqueue(redis_client, config, prefix, "first")
        assert client.get("/health").status_code == 503
        assert redis_client.llen(config.queues.input) == 1
        started = client.post("/start")
        assert started.status_code == 200
        assert started.json()["state"] == "running"
        first = wait_for_output(redis_client, config.queues.output)
        assert first.inspection.revision == 1
        assert client.post("/stop").json()["state"] == "stopped"
        updated = config.model_dump()
        updated["tags"] = ["updated"]
        assert client.put("/configuration", json=updated).status_code == 200
        enqueue(redis_client, config, prefix, "second")
        assert client.post("/start").status_code == 200
        second = wait_for_output(redis_client, config.queues.output)
        assert second.payload.tags == ["updated"]
        assert second.inspection.revision == 2
        assert client.get("/version").json()["name"] == "streamforge-core"
        assert client.post("/stop").json()["state"] == "stopped"


def test_control_api_uses_concrete_operator_schemas_and_rejects_live_reconfiguration(redis_setup):
    config, _, _ = redis_setup
    with TestClient(create_app(config, autostart=False)) as client:
        invalid = config.model_dump()
        invalid["branches"] = [
            {
                "name": "main",
                "operators": [
                    {"name": "tag", "parameters": {"tag": 42}},
                ],
            }
        ]
        assert client.put("/configuration", json=invalid).status_code == 422
        schema = client.get("/openapi.json").json()
        assert "TagParameters" in schema["components"]["schemas"]
        assert client.post("/start").status_code == 200
        assert client.put("/configuration", json=config.model_dump()).status_code == 409


def test_control_configuration_can_be_persisted_without_embedding_credentials(
    redis_setup, tmp_path
):
    from streamforge.application.configuration import load_configuration

    config, _, _ = redis_setup
    path = tmp_path / "settings.json"
    path.write_text(config.model_dump_json(), encoding="utf-8")
    with TestClient(create_app(config, autostart=False, configuration_path=path)) as client:
        updated = config.model_dump()
        updated["tags"] = ["persisted"]
        assert client.put("/configuration", json=updated).status_code == 200
    assert load_configuration(path.read_text(encoding="utf-8")).tags == ["persisted"]


def test_status_snapshot_survives_concurrent_stopped_configuration_change(redis_setup):
    config, _, _ = redis_setup
    controller = PausingStatusController(config)
    controller.entered = threading.Event()
    controller.release = threading.Event()
    controller.start()
    controller.stop()
    attempted = threading.Event()

    def read_status():
        controller.reader_id = threading.get_ident()
        return controller.status()

    def reconfigure():
        attempted.set()
        return controller.configure(config)

    with ThreadPoolExecutor(max_workers=2) as executor:
        reader = executor.submit(read_status)
        assert controller.entered.wait(5)
        writer = executor.submit(reconfigure)
        assert attempted.wait(5)
        try:
            writer.result(timeout=1)
        except FutureTimeout:
            pass
        finally:
            controller.release.set()
        assert reader.result(timeout=5).state == "stopped"
        assert writer.result(timeout=5).revision == 2


def test_control_openapi_and_configuration_support_multiple_plugin_types(redis_setup):
    from streamforge.application.configuration import default_registry
    from tests.test_inspection import FindingOperator

    config, _, _ = redis_setup
    operators = default_registry()
    operators.register("fixture", FindingOperator)
    with TestClient(create_app(config, operators=operators, autostart=False)) as client:
        schema = client.get("/openapi.json").json()["components"]["schemas"]
        assert "TagParameters" in schema and "FindingParameters" in schema
        updated = config.model_dump()
        updated["branches"] = [
            {
                "name": "main",
                "operators": [
                    {"name": "tag", "parameters": {"tag": "ready"}},
                    {"name": "fixture", "parameters": {"label": "one"}},
                ],
            }
        ]
        assert client.put("/configuration", json=updated).status_code == 200


def test_control_rejects_unrunnable_combined_worker_names_before_applying_configuration(
    redis_setup,
):
    config, _, _ = redis_setup
    with TestClient(create_app(config, autostart=False)) as client:
        invalid = config.model_dump()
        invalid["pipeline"]["name"] = "p" * 128
        assert client.put("/configuration", json=invalid).status_code == 422
        assert client.get("/configuration").json()["pipeline"]["name"] == config.pipeline.name
