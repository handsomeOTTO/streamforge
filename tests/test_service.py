# SPDX-FileCopyrightText: 2026 StreamForge contributors
# SPDX-License-Identifier: Apache-2.0

import base64
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

redis = pytest.importorskip("redis")

from streamforge.application.service import TaskService
from streamforge.messages import RejectedMessage, ResultMessage
from streamforge.models import ImagePayload, PipelineConfig, StageConfig
from streamforge.pipeline import Pipeline, Stage
from streamforge.transports.redis.transport import RedisTransport
from tests.support import FailImage, SleepImage, SleepParameters
from tests.test_redis_transport import make_task

pytestmark = pytest.mark.redis


def enqueue(client, config, prefix, name, *, content=b"opaque-image", encoding="base64", tags=None):
    task = make_task(prefix, name, encoding)
    task.tags = tags or []
    client.set(task.image.key, base64.b64encode(content) if encoding == "base64" else content)
    client.rpush(config.queues.input, task.model_dump_json())
    return task


def read_results(client, config):
    return [
        ResultMessage.model_validate_json(raw) for raw in client.lrange(config.queues.output, 0, -1)
    ]


def custom_pipeline(config, processor, parameters=None):
    kwargs = {} if parameters is None else {"parameters": parameters}
    return Pipeline(
        config=config.pipeline,
        payload_model=ImagePayload,
        stages=[Stage(StageConfig(name="custom"), processor, **kwargs)],
    )


def test_real_redis_to_cvo_to_redis_processes_multiple_images(redis_setup):
    config, client, prefix = redis_setup
    config.tags = ["processed"]
    for index in range(7):
        enqueue(
            client,
            config,
            prefix,
            f"frame-{index}",
            content=f"image-{index}".encode(),
            encoding="raw" if index % 2 else "base64",
            tags=["received"],
        )
    report = TaskService(config).run(max_messages=7)
    results = {result.task_id: result for result in read_results(client, config)}
    assert report.completed == 7 and report.failed == 0 and report.rejected == 0
    assert report.shutdown.graceful
    assert len(results) == 7
    for index in range(7):
        result = results[f"frame-{index}"]
        assert result.payload.image.to_bytes() == f"image-{index}".encode()
        assert result.payload.tags == ["received", "processed"]
        assert result.source_image.key == f"{prefix}:image:frame-{index}"
    assert client.llen(config.queues.input) == client.llen(config.queues.processing) == 0


def test_bad_messages_missing_images_and_invalid_base64_do_not_stop_service(redis_setup):
    config, client, prefix = redis_setup
    client.rpush(config.queues.input, b'{"unexpected":"private-value"}')
    missing = make_task(prefix, "missing")
    client.rpush(config.queues.input, missing.model_dump_json())
    invalid = enqueue(client, config, prefix, "invalid")
    client.set(invalid.image.key, b"not base64!")
    enqueue(client, config, prefix, "good")
    report = TaskService(config).run(max_messages=4)
    assert (report.completed, report.failed, report.rejected) == (3, 2, 1)
    results = {result.task_id: result for result in read_results(client, config)}
    assert results["missing"].status == results["invalid"].status == "failed"
    assert results["good"].status == "succeeded"
    rejected_raw = client.lpop(config.queues.rejected)
    assert RejectedMessage.model_validate_json(rejected_raw).error.error_type == "ValidationError"
    assert b"private-value" not in rejected_raw
    assert client.llen(config.queues.processing) == 0


def test_duplicate_in_flight_message_does_not_add_an_unpaired_receipt(redis_setup, caplog):
    config, client, prefix = redis_setup
    task = enqueue(client, config, prefix, "duplicate")
    client.rpush(config.queues.input, task.model_dump_json())
    pipeline = custom_pipeline(config, SleepImage, SleepParameters(seconds=0.2))
    with caplog.at_level(logging.INFO, logger="streamforge.application.service"):
        report = TaskService(config, pipeline=pipeline).run(max_messages=2)
    assert (report.completed, report.failed, report.rejected) == (1, 0, 1)
    assert len(read_results(client, config)) == 1
    rejection = RejectedMessage.model_validate_json(client.lpop(config.queues.rejected))
    assert rejection.error.error_type == "DuplicateTask"
    messages = [
        record.getMessage()
        for record in caplog.records
        if record.name == "streamforge.application.service"
        and record.levelno == logging.INFO
        and "task_id: duplicate" in record.getMessage()
    ]
    assert len(messages) == 2
    assert messages[0].startswith("↓ redis =>")
    assert messages[1].startswith("↑ redis =>")
    assert client.llen(config.queues.processing) == 0


def test_processor_errors_are_published_and_later_tasks_continue(redis_setup):
    config, client, prefix = redis_setup
    enqueue(client, config, prefix, "bad", tags=["fail"])
    enqueue(client, config, prefix, "good")
    report = TaskService(config, pipeline=custom_pipeline(config, FailImage)).run(max_messages=2)
    results = {result.task_id: result for result in read_results(client, config)}
    assert report.failed == 1
    assert results["bad"].error.error_type == "ValueError"
    assert results["good"].status == "succeeded"


def test_task_timeout_produces_failed_result_and_releases_local_capacity(redis_setup):
    config, client, prefix = redis_setup
    config.task_timeout = 0.02
    enqueue(client, config, prefix, "slow")
    pipeline = custom_pipeline(config, SleepImage, SleepParameters(seconds=0.15))
    report = TaskService(config, pipeline=pipeline).run(max_messages=1)
    assert report.completed == report.failed == 1
    assert read_results(client, config)[0].error.error_type == "TaskTimeout"
    assert pipeline.pending_count == 0
    assert client.llen(config.queues.processing) == 0


def test_full_processing_queue_applies_backpressure_without_failing_valid_tasks(redis_setup):
    config, client, prefix = redis_setup
    config.pipeline = PipelineConfig(queue_capacity=1, max_pending=5)
    for index in range(8):
        enqueue(client, config, prefix, f"task-{index}")
    pipeline = custom_pipeline(config, SleepImage, SleepParameters(seconds=0.03))
    report = TaskService(config, pipeline=pipeline).run(max_messages=8)
    assert report.completed == 8 and report.failed == 0
    assert all(result.status == "succeeded" for result in read_results(client, config))


def wait_for_claim(client, key):
    deadline = time.monotonic() + 10
    while not client.llen(key):
        if time.monotonic() > deadline:
            raise AssertionError("Service did not claim its task")
        time.sleep(0.01)


def test_stop_drains_inflight_work_before_exiting(redis_setup):
    config, client, prefix = redis_setup
    enqueue(client, config, prefix, "one")
    stop = threading.Event()
    pipeline = custom_pipeline(config, SleepImage, SleepParameters(seconds=0.05))
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(TaskService(config, pipeline=pipeline).run, stop)
        try:
            wait_for_claim(client, config.queues.processing)
        finally:
            stop.set()
        report = future.result(timeout=10)
    assert report.completed == 1 and report.abandoned == 0
    assert report.shutdown.graceful
    assert read_results(client, config)[0].status == "succeeded"


def test_forced_stop_retains_claims_for_explicit_recovery(redis_setup):
    config, client, prefix = redis_setup
    config.pipeline.shutdown_timeout = 0.1
    enqueue(client, config, prefix, "recoverable")
    stop = threading.Event()
    pipeline = custom_pipeline(config, SleepImage, SleepParameters(seconds=30.0))
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(TaskService(config, pipeline=pipeline).run, stop)
        try:
            wait_for_claim(client, config.queues.processing)
            time.sleep(0.03)
        finally:
            stop.set()
        report = future.result(timeout=10)
    assert report.abandoned == 1
    assert not report.shutdown.graceful
    assert client.llen(config.queues.processing) == 1
    assert client.llen(config.queues.output) == 0
    with RedisTransport(config) as transport:
        assert transport.recover() == 1
    assert TaskService(config).run(max_messages=1).completed == 1


def test_nonempty_processing_queue_requires_explicit_recovery(redis_setup):
    config, client, prefix = redis_setup
    raw = make_task(prefix).model_dump_json()
    client.rpush(config.queues.processing, raw)
    with pytest.raises(RuntimeError, match="recover"):
        TaskService(config).run(max_messages=1)
    assert client.lrange(config.queues.processing, 0, -1) == [raw.encode()]


def test_service_publication_error_keeps_claim_available_for_recovery(redis_setup):
    config, client, prefix = redis_setup
    task = enqueue(client, config, prefix, "one")
    client.set(config.queues.output, "wrong-type")
    with pytest.raises(redis.ResponseError):
        TaskService(config).run(max_messages=1)
    assert client.lrange(config.queues.processing, 0, -1) == [task.model_dump_json().encode()]


def test_redis_workflow_dispatches_and_aggregates_registered_inspection_branches(redis_setup):
    from tests.test_inspection import branch, registry

    config, client, prefix = redis_setup
    config.branches = [branch("a"), branch("b")]
    enqueue(client, config, prefix, "full-workflow")
    report = TaskService(config, operators=registry()).run(max_messages=1)
    assert report.completed == 1
    result = read_results(client, config)[0]
    assert [finding.label for finding in result.inspection.findings] == ["a", "b"]
    assert [outcome.branch for outcome in result.inspection.branches] == ["a", "b"]


def test_health_tasks_follow_the_processing_path_and_use_health_result_queue(redis_setup):
    config, client, prefix = redis_setup
    config.tags = ["self-test"]
    task = make_task(prefix, "health")
    task.kind = "health"
    client.set(task.image.key, base64.b64encode(b"health-image"))
    client.rpush(config.queues.input, task.model_dump_json())
    report = TaskService(config).run(max_messages=1)
    assert report.completed == 1
    assert client.llen(config.queues.output) == 0
    result = ResultMessage.model_validate_json(client.lpop(config.queues.health_output))
    assert result.kind == "health" and result.payload.tags == ["self-test"]
    assert result.inspection.branches[0].status == "succeeded"


@pytest.mark.parametrize("size_megabytes", [1, 4])
def test_large_valid_image_reaches_workers_instead_of_expiring_during_serialization(
    redis_setup, size_megabytes
):
    config, client, prefix = redis_setup
    config.task_timeout = 5.0
    content = b"\x00" * (size_megabytes * 1024 * 1024)
    enqueue(client, config, prefix, "large", content=content)
    report = TaskService(config).run(max_messages=1)
    assert report.failed == 0
    assert read_results(client, config)[0].payload.image.to_bytes() == content


def test_full_inspection_and_health_workflow_across_redis_databases(redis_output_setup):
    from tests.test_inspection import branch, registry

    config, client, output, prefix = redis_output_setup
    config.branches = [branch("a"), branch("b")]
    enqueue(client, config, prefix, "inspection")
    task = make_task(prefix, "health")
    task.kind = "health"
    client.set(task.image.key, base64.b64encode(b"health-image"))
    client.rpush(config.queues.input, task.model_dump_json(), b'{"invalid":"redacted"}')
    report = TaskService(config, operators=registry()).run(max_messages=3)
    assert (report.completed, report.failed, report.rejected) == (2, 0, 1)
    result = read_results(output, config)[0]
    assert result.payload.image.to_bytes() == b"opaque-image"
    assert [finding.label for finding in result.inspection.findings] == ["a", "b"]
    health = ResultMessage.model_validate_json(output.lpop(config.queues.health_output))
    assert health.kind == "health" and health.payload.image.to_bytes() == b"health-image"
    assert client.llen(config.queues.processing) == client.llen(config.queues.output) == 0
    assert client.llen(config.queues.rejected) == 1
    assert output.llen(config.queues.rejected) == 0
