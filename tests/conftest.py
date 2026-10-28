# SPDX-FileCopyrightText: 2026 StreamForge contributors
# SPDX-License-Identifier: Apache-2.0

"""Opt-in real Redis fixture; never flush or mutate unrelated keys."""

import os
import socket
import subprocess
import time
from urllib.parse import urlparse
from uuid import uuid4

import pytest


@pytest.fixture(scope="session")
def redis_url(tmp_path_factory):
    redis = pytest.importorskip("redis")
    configured = os.environ.get("STREAMFORGE_TEST_REDIS_URL")
    if configured:
        yield configured
        return
    executable = os.environ.get("STREAMFORGE_REDIS_SERVER")
    if not executable:
        pytest.skip(
            "Set STREAMFORGE_REDIS_SERVER or STREAMFORGE_TEST_REDIS_URL for real Redis tests"
        )
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    directory = tmp_path_factory.mktemp("redis-server")
    with (directory / "server.log").open("wb") as log:
        process = subprocess.Popen(
            [
                executable,
                "--bind",
                "127.0.0.1",
                "--port",
                str(port),
                "--save",
                "",
                "--appendonly",
                "no",
                "--protected-mode",
                "yes",
            ],
            cwd=directory,
            stdout=log,
            stderr=subprocess.STDOUT,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
        )
        client = redis.Redis(host="127.0.0.1", port=port, socket_connect_timeout=0.2)
        try:
            deadline = time.monotonic() + 10
            while True:
                try:
                    client.ping()
                    break
                except redis.RedisError:
                    if process.poll() is not None or time.monotonic() > deadline:
                        pytest.fail("Isolated Redis server failed to start; inspect its server.log")
                    time.sleep(0.05)
            yield f"redis://127.0.0.1:{port}/0"
        finally:
            client.close()
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


@pytest.fixture
def redis_setup(redis_url):
    import redis

    from streamforge.application.models import ServiceConfig
    from streamforge.models import PipelineConfig
    from streamforge.transports.redis.models import RedisConnection, RedisQueues

    parsed = urlparse(redis_url)
    if parsed.username or parsed.password or parsed.scheme != "redis":
        pytest.fail("Tests require a dedicated local Redis URL without embedded credentials")
    prefix = f"streamforge-test:{uuid4().hex}"
    config = ServiceConfig(
        connection=RedisConnection(
            host=parsed.hostname, port=parsed.port or 6379, db=int(parsed.path.lstrip("/") or "0")
        ),
        queues=RedisQueues(
            input=f"{prefix}:input",
            processing=f"{prefix}:processing",
            output=f"{prefix}:output",
            rejected=f"{prefix}:rejected",
            health_output=f"{prefix}:health",
        ),
        pipeline=PipelineConfig(queue_capacity=2, max_pending=3),
        workers=2,
        poll_interval=0.01,
    )
    client = redis.Redis.from_url(redis_url)
    try:
        yield config, client, prefix
    finally:
        owned = list(client.scan_iter(match=f"{prefix}:*"))
        if owned:
            client.delete(*owned)
        client.close()


@pytest.fixture
def redis_output_setup(redis_setup):
    import redis

    config, client, prefix = redis_setup
    output_db = 1 if config.connection.db == 0 else 0
    config.output_db = output_db
    output = redis.Redis(host=config.connection.host, port=config.connection.port, db=output_db)
    try:
        yield config, client, output, prefix
    finally:
        owned = list(output.scan_iter(match=f"{prefix}:*"))
        if owned:
            output.delete(*owned)
        output.close()
