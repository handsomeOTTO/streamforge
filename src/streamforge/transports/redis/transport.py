# SPDX-FileCopyrightText: 2026 StreamForge contributors
# SPDX-License-Identifier: Apache-2.0

"""Redis List claims, opaque image lookup, and acknowledged result publication."""

import os

import redis
from redis.backoff import NoBackoff
from redis.retry import Retry

from ...messages import ImageReference, RejectedMessage, ResultMessage
from ...models import ImageData
from .models import RedisConnection, RedisQueues, RedisTransportConfig

_PUBLISH = """
if not redis.call('LPOS', KEYS[1], ARGV[1]) then
    return 0
end
redis.call('RPUSH', KEYS[2], ARGV[2])
redis.call('LREM', KEYS[1], 1, ARGV[1])
return 1
"""


class RedisTransport:
    def __init__(self, config: RedisTransportConfig):
        self.connection = RedisConnection.model_validate_json(config.connection.model_dump_json())
        self.queues = RedisQueues.model_validate_json(config.queues.model_dump_json())
        self.output_db = config.output_db
        self._client = None
        self._output_client = None

    def connect(self):
        if self._client is not None:
            raise RuntimeError("Redis transport is already connected")
        connection = self.connection
        password = None
        if connection.password_env:
            password = os.environ.get(connection.password_env)
            if not password:
                raise ValueError("Configured Redis credential environment variable is not set")
        options = dict(
            host=connection.host,
            port=connection.port,
            username=connection.username,
            password=password,
            ssl=connection.tls,
            socket_connect_timeout=connection.connect_timeout,
            socket_timeout=connection.socket_timeout,
            decode_responses=False,
            retry=Retry(NoBackoff(), 0),
        )
        self._client = redis.Redis(db=connection.db, **options)
        self._output_client = self._client
        try:
            self._client.ping()
            if self.output_db is not None and self.output_db != connection.db:
                self._output_client = redis.Redis(db=self.output_db, **options)
                self._output_client.ping()
        except BaseException:
            self.close()
            raise
        return self

    @property
    def client(self):
        if self._client is None:
            raise RuntimeError("Redis transport is not connected")
        return self._client

    @property
    def output_client(self):
        if self._output_client is None:
            raise RuntimeError("Redis transport is not connected")
        return self._output_client

    @property
    def processing_count(self) -> int:
        return self.client.llen(self.queues.processing)

    def claim(self) -> bytes | None:
        queues = self.queues
        return self.client.lmove(queues.input, queues.processing, "LEFT", "RIGHT")

    def load_image(self, reference: ImageReference) -> ImageData:
        reference = ImageReference.model_validate_json(reference.model_dump_json())
        try:
            content = self.client.get(reference.key)
        except redis.ResponseError as exc:
            if "WRONGTYPE" in str(exc).upper():
                raise ValueError("Referenced image must be a Redis string") from exc
            raise
        if content is None:
            raise FileNotFoundError("Referenced image does not exist")
        if reference.encoding == "raw":
            return ImageData.from_bytes(content, media_type=reference.media_type)
        return ImageData(media_type=reference.media_type, content_base64=content.decode("ascii"))

    def publish(self, raw: bytes, message: ResultMessage | RejectedMessage) -> bool:
        if not isinstance(raw, bytes):
            raise TypeError("Claimed message must be bytes")
        queues = self.queues
        if isinstance(message, ResultMessage):
            model = ResultMessage
            destination = queues.health_output if message.kind == "health" else queues.output
        elif isinstance(message, RejectedMessage):
            model, destination = RejectedMessage, queues.rejected
        else:
            raise TypeError("Publication requires ResultMessage or RejectedMessage")
        validated = model.model_validate_json(message.model_dump_json(warnings=False))
        target = self.output_client if isinstance(message, ResultMessage) else self.client
        if target is not self.client:
            if self.client.lpos(queues.processing, raw) is None:
                return False
            # Cross-database delivery is at least once: publish before acknowledging.
            target.rpush(destination, validated.model_dump_json())
            return self.client.lrem(queues.processing, 1, raw) == 1
        return (
            self.client.eval(
                _PUBLISH,
                2,
                queues.processing,
                destination,
                raw,
                validated.model_dump_json(),
            )
            == 1
        )

    def recover(self) -> int:
        """Requeue unfinished work in FIFO order; call only when its owner is stopped."""
        recovered = 0
        queues = self.queues
        while self.client.lmove(queues.processing, queues.input, "RIGHT", "LEFT") is not None:
            recovered += 1
        return recovered

    def close(self):
        if self._output_client is not None and self._output_client is not self._client:
            self._output_client.close()
        self._output_client = None
        if self._client is not None:
            self._client.close()
            self._client = None

    def __enter__(self):
        return self.connect()

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
