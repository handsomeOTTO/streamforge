# SPDX-FileCopyrightText: 2026 StreamForge contributors
# SPDX-License-Identifier: Apache-2.0

"""Strict task and result envelopes shared by applications and transports."""

from typing import Annotated, Literal

from pydantic import Field

from .inspection.models import InspectionSummary
from .models import ImagePayload, Name, ProcessingError, StrictModel, TaskResult


class ImageReference(StrictModel):
    key: Annotated[str, Field(min_length=1, max_length=512)]
    media_type: Literal["image/png", "image/jpeg", "image/bmp", "image/tiff", "image/webp"]
    encoding: Literal["base64", "raw"] = "base64"


class TaskMessage(StrictModel):
    task_id: Name
    image: ImageReference
    tags: list[Annotated[str, Field(max_length=256)]] = Field(default_factory=list, max_length=64)
    targets: list[Name] = Field(default_factory=list)
    kind: Literal["inspection", "health"] = "inspection"


class ResultMessage(TaskResult[ImagePayload]):
    source_image: ImageReference
    kind: Literal["inspection", "health"] = "inspection"
    inspection: InspectionSummary | None = None


class RejectedMessage(StrictModel):
    message_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    error: ProcessingError
