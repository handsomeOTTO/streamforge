# SPDX-FileCopyrightText: 2026 StreamForge contributors
# SPDX-License-Identifier: Apache-2.0

import json
import os
import re
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

from streamforge.messages import ResultMessage, TaskMessage
from tests.test_cli import run_cli
from tests.test_redis_transport import make_task
from tests.test_service import enqueue

pytestmark = pytest.mark.redis


def test_redis_cli_reads_image_key_and_publishes_result(redis_setup, tmp_path):
    config, client, prefix = redis_setup
    enqueue(client, config, prefix, "cli")
    settings = tmp_path / "redis.json"
    settings.write_text(config.model_dump_json(), encoding="utf-8")
    completed = run_cli("redis", "--config", str(settings), "--max-messages", "1")
    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout)["completed"] == 1
    assert "↓ redis =>" in completed.stderr
    assert "↑ redis =>" in completed.stderr
    result = json.loads(client.lpop(config.queues.output))
    assert result["task_id"] == "cli" and result["status"] == "succeeded"


def test_redis_recover_cli_requeues_unfinished_work(redis_setup, tmp_path):
    config, client, prefix = redis_setup
    raw = make_task(prefix).model_dump_json()
    client.rpush(config.queues.processing, raw)
    settings = tmp_path / "redis.json"
    settings.write_text(config.model_dump_json(), encoding="utf-8")
    completed = run_cli("redis-recover", "--config", str(settings))
    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout) == {"recovered": 1}
    assert client.lpop(config.queues.input) == raw.encode()


def test_invalid_redis_configuration_does_not_print_embedded_credentials(tmp_path):
    settings = tmp_path / "bad.json"
    settings.write_text('{"connection":{"password":"must-not-print-this"}}', encoding="utf-8")
    completed = run_cli("redis", "--config", str(settings))
    assert completed.returncode != 0
    assert "must-not-print-this" not in completed.stderr


def test_root_main_and_independent_client_complete_cross_database_workflow(
    redis_output_setup, tmp_path
):
    config, input_client, output_client, prefix = redis_output_setup
    config.tags = ["processed"]
    root = Path(__file__).resolve().parents[1]
    settings = tmp_path / "service.json"
    settings.write_text(config.model_dump_json(), encoding="utf-8")
    image = tmp_path / "sample.png"
    image.write_bytes(b"\x00\xffopaque-image")
    output = tmp_path / "received"
    other = (
        ResultMessage(
            task_id="other",
            status="failed",
            source_image=make_task(prefix, "other").image,
            error={"stage": "example", "error_type": "Example", "message": "Unrelated result"},
        )
        .model_dump_json()
        .encode()
    )
    output_client.rpush(config.queues.output, other)
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    with (tmp_path / "server.log").open("wb") as log:
        process = subprocess.Popen(
            [sys.executable, str(root / "main.py"), "--config", str(settings), "--port", str(port)],
            cwd=tmp_path,
            stdout=log,
            stderr=subprocess.STDOUT,
            env={**os.environ, "PYTHONIOENCODING": "ascii"},
        )
        try:
            deadline = time.monotonic() + 15
            while True:
                try:
                    with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=1):
                        break
                except (OSError, urllib.error.URLError):
                    assert process.poll() is None, "Root main exited before becoming ready"
                    assert time.monotonic() < deadline, "Root main did not become ready"
                    time.sleep(0.05)
            completed = subprocess.run(
                [
                    sys.executable,
                    str(root / "simulate_client.py"),
                    "--config",
                    str(settings),
                    "--image",
                    str(image),
                    "--output-dir",
                    str(output),
                    "--timeout",
                    "10",
                ],
                cwd=tmp_path,
                capture_output=True,
                text=True,
                timeout=15,
            )
            assert completed.returncode == 0, completed.stderr
            stored = ResultMessage.model_validate_json(next(output.glob("*.json")).read_text())
            assert stored.status == "succeeded"
            assert stored.payload.tags == ["processed"]
            assert next(output.glob("*.png")).read_bytes() == image.read_bytes()
            assert output_client.lrange(config.queues.output, 0, -1) == [other]
            assert input_client.llen(config.queues.processing) == 0
            assert input_client.get(stored.source_image.key) == image.read_bytes()
            assert process.poll() is None
            deadline = time.monotonic() + 5
            while True:
                service_log = (tmp_path / "server.log").read_text(encoding="utf-8")
                file_log = (tmp_path / "logs" / "streamforge.log").read_text(encoding="utf-8")
                published = f"| task_id: {stored.task_id} status:"
                if published in service_log and published in file_log:
                    break
                assert time.monotonic() < deadline, "Task lifecycle logs are missing from main"
                time.sleep(0.05)
            service_log = re.sub(r"\x1b\[[0-9;]*m", "", service_log)
            task_lines = [
                line for line in service_log.splitlines() if f"task_id: {stored.task_id}" in line
            ]
            assert len(task_lines) == 2
            assert "↓ redis =>" in task_lines[0]
            assert "↑ redis =>" in task_lines[1]
            assert repr(stored.source_image.key) in task_lines[0]
            assert repr(stored.source_image.key) in task_lines[1]
            assert re.search(
                r"\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}\]"
                r"\[service\t\]\[\d+\]\[INFO\]:",
                service_log,
            )
            assert re.search(r"\bpid: \d+\b", service_log)
            assert "thread: task-service" in service_log
            assert "cost:" in service_log
            assert (
                len(
                    [line for line in file_log.splitlines() if f"task_id: {stored.task_id}" in line]
                )
                == 2
            )
            assert "\x1b[" not in file_log
            assert "opaque-image" not in service_log
        finally:
            try:
                request = urllib.request.Request(f"http://127.0.0.1:{port}/stop", method="POST")
                with urllib.request.urlopen(request, timeout=10):
                    pass
            except OSError:
                pass
            process.terminate()
            process.wait(timeout=10)


def test_independent_client_timeout_retains_queued_task_and_image(redis_setup, tmp_path):
    config, client, _ = redis_setup
    root = Path(__file__).resolve().parents[1]
    settings = tmp_path / "client.json"
    settings.write_text(config.model_dump_json(), encoding="utf-8")
    completed = subprocess.run(
        [
            sys.executable,
            str(root / "simulate_client.py"),
            "--config",
            str(settings),
            "--timeout",
            "0.1",
            "--output-dir",
            str(tmp_path / "received"),
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert completed.returncode == 1
    assert "Timed out" in completed.stderr
    task = TaskMessage.model_validate_json(client.lindex(config.queues.input, 0))
    assert client.get(task.image.key) is not None
