# SPDX-FileCopyrightText: 2026 StreamForge contributors
# SPDX-License-Identifier: Apache-2.0

import base64

import pytest

redis = pytest.importorskip("redis")

from streamforge.messages import ImageReference, ResultMessage, TaskMessage
from streamforge.models import ImagePayload
from streamforge.transports.redis.transport import RedisTransport

pytestmark = pytest.mark.redis


def make_task(prefix, task_id="one", encoding="base64"):
    return TaskMessage(
        task_id=task_id,
        image=ImageReference(
            key=f"{prefix}:image:{task_id}",
            media_type="image/png",
            encoding=encoding,
        ),
    )


def test_transport_claim_load_publish_and_acknowledge_real_redis(redis_setup):
    config, client, prefix = redis_setup
    task = make_task(prefix)
    raw = task.model_dump_json().encode()
    client.set(task.image.key, base64.b64encode(b"opaque-image"))
    client.rpush(config.queues.input, raw)
    with RedisTransport(config) as transport:
        assert transport.claim() == raw
        assert client.llen(config.queues.input) == 0
        assert client.lrange(config.queues.processing, 0, -1) == [raw]
        image = transport.load_image(task.image)
        result = ResultMessage(
            task_id="one",
            status="succeeded",
            source_image=task.image,
            payload=ImagePayload(image=image),
        )
        assert transport.publish(raw, result) is True
        assert transport.publish(raw, result) is False
    assert client.llen(config.queues.processing) == 0
    published = ResultMessage.model_validate_json(client.lpop(config.queues.output))
    assert published.payload.image.to_bytes() == b"opaque-image"
    assert client.llen(config.queues.output) == 0


def test_transport_preserves_claim_if_result_publication_fails(redis_setup):
    config, client, prefix = redis_setup
    task = make_task(prefix)
    raw = task.model_dump_json().encode()
    client.rpush(config.queues.input, raw)
    client.set(config.queues.output, "wrong-type")
    with RedisTransport(config) as transport:
        assert transport.claim() == raw
        result = ResultMessage(
            task_id="one",
            status="failed",
            source_image=task.image,
            error={"stage": "test", "error_type": "MissingImage", "message": "missing"},
        )
        with pytest.raises(redis.ResponseError):
            transport.publish(raw, result)
    assert client.lrange(config.queues.processing, 0, -1) == [raw]


def test_transport_recovers_unfinished_claims_in_fifo_order(redis_setup):
    config, client, prefix = redis_setup
    messages = [make_task(prefix, name).model_dump_json().encode() for name in ("a", "b", "c")]
    client.rpush(config.queues.input, *messages)
    with RedisTransport(config) as transport:
        assert transport.claim() == messages[0]
        assert transport.claim() == messages[1]
        assert transport.recover() == 2
    assert client.lrange(config.queues.input, 0, -1) == messages
    assert client.llen(config.queues.processing) == 0


def test_transport_supports_raw_image_bytes_without_pixel_decoding(redis_setup):
    config, client, prefix = redis_setup
    task = make_task(prefix, encoding="raw")
    client.set(task.image.key, b"\x00\xffopaque")
    with RedisTransport(config) as transport:
        assert transport.load_image(task.image).to_bytes() == b"\x00\xffopaque"


def test_image_reference_preserves_valid_redis_keys_with_spaces(redis_setup):
    config, client, prefix = redis_setup
    key = f"{prefix}:image:valid image key"
    client.set(key, base64.b64encode(b"image-with-space-key"))
    reference = ImageReference(key=key, media_type="image/png")
    with RedisTransport(config) as transport:
        assert transport.load_image(reference).to_bytes() == b"image-with-space-key"


def test_transport_publishes_to_a_separate_database_and_retains_input_selection(redis_output_setup):
    config, client, output, prefix = redis_output_setup
    task = make_task(prefix)
    raw = task.model_dump_json().encode()
    client.rpush(config.queues.input, raw)
    with RedisTransport(config) as transport:
        assert transport.claim() == raw
        result = ResultMessage(
            task_id=task.task_id,
            status="failed",
            source_image=task.image,
            error={"stage": "test", "error_type": "MissingImage", "message": "missing"},
        )
        assert transport.publish(raw, result)
        assert not transport.publish(raw, result)
        assert transport.processing_count == 0
        assert transport.output_client.lrange(config.queues.output, 0, -1) == [
            result.model_dump_json().encode()
        ]
    assert client.llen(config.queues.output) == 0
    assert output.llen(config.queues.output) == 1


def test_cross_database_publication_failure_preserves_claim(redis_output_setup):
    config, client, output, prefix = redis_output_setup
    task = make_task(prefix)
    raw = task.model_dump_json().encode()
    client.rpush(config.queues.input, raw)
    output.set(config.queues.output, "wrong-type")
    with RedisTransport(config) as transport:
        assert transport.claim() == raw
        result = ResultMessage(
            task_id=task.task_id,
            status="failed",
            source_image=task.image,
            error={"stage": "test", "error_type": "MissingImage", "message": "missing"},
        )
        with pytest.raises(redis.ResponseError):
            transport.publish(raw, result)
    assert client.lrange(config.queues.processing, 0, -1) == [raw]


def test_cross_database_acknowledgement_failure_allows_replay(redis_output_setup, monkeypatch):
    config, client, output, prefix = redis_output_setup
    task = make_task(prefix)
    raw = task.model_dump_json().encode()
    client.rpush(config.queues.input, raw)
    result = ResultMessage(
        task_id=task.task_id,
        status="failed",
        source_image=task.image,
        error={"stage": "test", "error_type": "MissingImage", "message": "missing"},
    )

    def interrupted(*args, **kwargs):
        raise redis.ConnectionError("Injected failure after output publication")

    with RedisTransport(config) as transport:
        assert transport.claim() == raw
        with monkeypatch.context() as context:
            context.setattr(transport.client, "lrem", interrupted)
            with pytest.raises(redis.ConnectionError):
                transport.publish(raw, result)
        assert output.llen(config.queues.output) == 1
        assert client.lrange(config.queues.processing, 0, -1) == [raw]
        assert transport.recover() == 1
        assert transport.claim() == raw
        assert transport.publish(raw, result)
    assert output.llen(config.queues.output) == 2
    assert client.llen(config.queues.processing) == 0
