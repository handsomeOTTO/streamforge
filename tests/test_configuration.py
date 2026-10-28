# SPDX-FileCopyrightText: 2026 StreamForge contributors
# SPDX-License-Identifier: Apache-2.0

import pytest
from pydantic import ValidationError

from streamforge.application.models import ServiceConfig
from streamforge.messages import TaskMessage
from streamforge.transports.redis.models import RedisConnection, RedisQueues


@pytest.mark.parametrize(
    "raw",
    [
        '{"task_id":"one","image":{"key":"image:one","media_type":"image/png","extra":0}}',
        '{"task_id":"one","image":{"key":"image:one","media_type":"image/png"},"extra":0}',
        '{"image":{"key":"image:one","media_type":"image/png"}}',
        '{"task_id":"one","image":{"key":"","media_type":"image/png"}}',
        '{"task_id":"one","image":{"key":"image:one","media_type":"image/png","encoding":"pickle"}}',
    ],
)
def test_redis_task_rejects_invalid_or_untyped_input(raw):
    with pytest.raises(ValidationError):
        TaskMessage.model_validate_json(raw)


def test_redis_configuration_rejects_coercion_shared_queue_keys_and_invalid_limits():
    with pytest.raises(ValidationError):
        RedisConnection(port="6379")
    with pytest.raises(ValidationError):
        RedisQueues(input="same", output="same")
    with pytest.raises(ValidationError):
        ServiceConfig(workers=0)
    with pytest.raises(ValidationError):
        ServiceConfig(poll_interval=0.0)
    with pytest.raises(ValidationError):
        ServiceConfig(output_db="1")
    with pytest.raises(ValidationError):
        ServiceConfig(output_db=-1)
