# SPDX-FileCopyrightText: 2026 StreamForge contributors
# SPDX-License-Identifier: Apache-2.0

"""Public data contracts and private transport envelopes."""

import base64
import binascii
from typing import Annotated, Generic, Literal, TypeVar
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", validate_assignment=True)


PayloadT = TypeVar("PayloadT", bound=StrictModel)
ParametersT = TypeVar("ParametersT", bound=StrictModel)
Name = Annotated[str, Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.-]+$")]
PositiveSeconds = Annotated[float, Field(gt=0, allow_inf_nan=False)]


class EmptyParameters(StrictModel):
    pass


class Task(StrictModel, Generic[PayloadT]):
    task_id: Name = Field(default_factory=lambda: uuid4().hex)
    payload: PayloadT


class ProcessingError(StrictModel):
    stage: str
    error_type: str
    message: str


class TaskResult(StrictModel, Generic[PayloadT]):
    task_id: Name
    status: Literal["succeeded", "failed"]
    payload: PayloadT | None = None
    error: ProcessingError | None = None

    @model_validator(mode="after")
    def check_outcome(self):
        if self.status == "succeeded":
            if self.payload is None or self.error is not None:
                raise ValueError("Success requires a payload and no error")
        elif self.payload is not None or self.error is None:
            raise ValueError("Failure requires an error and no payload")
        return self


class PipelineConfig(StrictModel):
    name: Name = "pipeline"
    queue_capacity: int = Field(default=64, ge=1)
    max_pending: int = Field(default=128, ge=1)
    start_timeout: PositiveSeconds = 15.0
    shutdown_timeout: PositiveSeconds = 10.0


class StageConfig(StrictModel):
    name: Name
    workers: int = Field(default=1, ge=1)


class ImageData(StrictModel):
    """An opaque image container; media type does not certify image contents."""

    media_type: Literal["image/png", "image/jpeg", "image/bmp", "image/tiff", "image/webp"]
    content_base64: str = Field(min_length=1, max_length=22_369_624)

    @field_validator("content_base64")
    @classmethod
    def check_base64(cls, value: str) -> str:
        try:
            raw = base64.b64decode(value, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise ValueError("Image content must be standard Base64") from exc
        if not raw or len(raw) > 16 * 1024 * 1024:
            raise ValueError("Image content must contain 1 to 16777216 bytes")
        if base64.b64encode(raw).decode("ascii") != value:
            raise ValueError("Image content must use canonical Base64 encoding")
        return value

    @classmethod
    def from_bytes(cls, content: bytes, *, media_type: str):
        if not isinstance(content, bytes):
            raise TypeError("Image content must be bytes")
        return cls(media_type=media_type, content_base64=base64.b64encode(content).decode("ascii"))

    def to_bytes(self) -> bytes:
        return base64.b64decode(self.content_base64, validate=True)


class ImagePayload(StrictModel):
    image: ImageData
    tags: list[str] = Field(default_factory=list)


class ShutdownReport(StrictModel):
    graceful: bool
    forced_workers: list[str] = Field(default_factory=list)
    error: ProcessingError | None = None


class WireTask(StrictModel):
    dispatch_id: str
    task_id: Name
    payload_json: str
    error: ProcessingError | None = None


class WorkerEvent(StrictModel):
    kind: Literal["ready", "failed"]
    worker: str
    error: ProcessingError | None = None
