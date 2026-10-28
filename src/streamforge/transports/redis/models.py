# SPDX-FileCopyrightText: 2026 StreamForge contributors
# SPDX-License-Identifier: Apache-2.0

"""Redis connection, queue configuration, and recovery reports."""

from typing import Annotated

from pydantic import Field, model_validator

from ...models import PositiveSeconds, StrictModel

RedisKey = Annotated[str, Field(min_length=1, max_length=512)]


class RedisConnection(StrictModel):
    host: str = Field(default="127.0.0.1", min_length=1, max_length=253)
    port: int = Field(default=6379, ge=1, le=65535)
    db: int = Field(default=0, ge=0)
    username: str | None = None
    password_env: str | None = Field(default=None, min_length=1)
    tls: bool = False
    connect_timeout: PositiveSeconds = 3.0
    socket_timeout: PositiveSeconds = 3.0


class RedisQueues(StrictModel):
    input: RedisKey = "streamforge:tasks"
    processing: RedisKey = "streamforge:processing"
    output: RedisKey = "streamforge:results"
    rejected: RedisKey = "streamforge:rejected"
    health_output: RedisKey = "streamforge:health:results"

    @model_validator(mode="after")
    def distinct_queues(self):
        if len({self.input, self.processing, self.output, self.rejected, self.health_output}) != 5:
            raise ValueError("Redis queue keys must be distinct")
        return self


class RedisTransportConfig(StrictModel):
    connection: RedisConnection = Field(default_factory=RedisConnection)
    output_db: int | None = Field(default=None, ge=0)
    queues: RedisQueues = Field(default_factory=RedisQueues)


class RedisRecoveryReport(StrictModel):
    recovered: int = Field(ge=0)
