# SPDX-FileCopyrightText: 2026 StreamForge contributors
# SPDX-License-Identifier: Apache-2.0

"""Detection data contracts without geometry computation or detection algorithms."""

from typing import Literal

from pydantic import Field, model_validator

from ..models import ImagePayload, Name, ProcessingError, StrictModel, Task, TaskResult
from ..operators import OperatorSpec


class Point(StrictModel):
    x: float = Field(allow_inf_nan=False)
    y: float = Field(allow_inf_nan=False)


class BoundingBox(StrictModel):
    x: float = Field(allow_inf_nan=False)
    y: float = Field(allow_inf_nan=False)
    width: float = Field(gt=0, allow_inf_nan=False)
    height: float = Field(gt=0, allow_inf_nan=False)


class Measurement(StrictModel):
    name: Name
    value: float = Field(allow_inf_nan=False)
    unit: str = Field(default="", max_length=32)


class Finding(StrictModel):
    label: str = Field(min_length=1, max_length=128)
    confidence: float = Field(ge=0, le=1, allow_inf_nan=False)
    box: BoundingBox | None = None
    polygon: list[Point] = Field(default_factory=list)
    measurements: list[Measurement] = Field(default_factory=list)

    @model_validator(mode="after")
    def valid_polygon(self):
        if self.polygon and len(self.polygon) < 3:
            raise ValueError("A polygon requires at least three points")
        return self


class BranchConfig(StrictModel):
    name: Name
    workers: int | None = Field(default=None, ge=1)
    operators: list[OperatorSpec] = Field(default_factory=list)


class InspectionPayload(ImagePayload):
    targets: list[Name] = Field(default_factory=list)
    findings: list[Finding] = Field(default_factory=list)


class BranchOutcome(StrictModel):
    branch: Name
    status: Literal["succeeded", "failed"]
    findings: list[Finding] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    error: ProcessingError | None = None

    @model_validator(mode="after")
    def valid_outcome(self):
        if (self.status == "failed") != (self.error is not None):
            raise ValueError("Branch failure requires an error; success cannot have one")
        return self


class InspectionSummary(StrictModel):
    revision: int = Field(default=1, ge=1)
    findings: list[Finding] = Field(default_factory=list)
    branches: list[BranchOutcome] = Field(default_factory=list)


class InspectionResult(TaskResult[InspectionPayload]):
    inspection: InspectionSummary


class PendingInspection(StrictModel):
    task: Task[InspectionPayload]
    expected: list[Name]
    submitted: set[Name] = Field(default_factory=set)
    outcomes: dict[str, BranchOutcome] = Field(default_factory=dict)
