# SPDX-FileCopyrightText: 2026 StreamForge contributors
# SPDX-License-Identifier: Apache-2.0

import time

import pytest
from fastapi.testclient import TestClient

from examples.redis_demo import create_demo_app

pytestmark = pytest.mark.redis


def test_browser_demo_submits_real_tasks_and_displays_cross_database_results(redis_output_setup):
    config, input_client, output_client, _ = redis_output_setup
    with TestClient(create_demo_app(config)) as client:
        assert client.get("/").status_code == 200
        submitted = client.post("/demo/tasks")
        assert submitted.status_code == 200
        identifiers = submitted.json()["task_ids"]
        deadline = time.monotonic() + 10
        while True:
            snapshot = client.get("/demo/results").json()
            if len(snapshot["results"]) == 3 and snapshot["status"]["completed"] == 3:
                break
            assert time.monotonic() < deadline, snapshot
            time.sleep(0.01)
        assert {result["task_id"] for result in snapshot["results"]} == set(identifiers)
        assert all(result["status"] == "succeeded" for result in snapshot["results"])
        assert sorted(result["kind"] for result in snapshot["results"]) == [
            "health",
            "inspection",
            "inspection",
        ]
        assert snapshot["status"]["worker_pids"]
        assert snapshot["input_db"] != snapshot["output_db"]
        assert input_client.llen(config.queues.processing) == 0
        assert input_client.llen(config.queues.output) == 0
        assert output_client.llen(config.queues.output) == 2
        assert client.post("/stop").status_code == 200
        assert client.post("/demo/tasks").status_code == 503
