# SPDX-FileCopyrightText: 2026 StreamForge contributors
# SPDX-License-Identifier: Apache-2.0

"""Application settings and task-service runtime reports."""

from typing import Literal

from pydantic import Field, model_validator

from ..inspection.models import BranchConfig
from ..models import PipelineConfig, PositiveSeconds, ProcessingError, ShutdownReport, StrictModel
from ..transports.redis.models import RedisTransportConfig


class ServiceConfig(RedisTransportConfig):
    pipeline: PipelineConfig = Field(default_factory=PipelineConfig)
    workers: int = Field(default=1, ge=1)
    tags: list[str] = Field(default_factory=list)
    poll_interval: PositiveSeconds = 0.05
    task_timeout: PositiveSeconds = 30.0
    branches: list[BranchConfig] = Field(
        default_factory=lambda: [BranchConfig(name="main")], min_length=1
    )

    @model_validator(mode="after")
    def unique_branches(self):
        if len({branch.name for branch in self.branches}) != len(self.branches):
            raise ValueError("Inspection branch names must be unique")
        if any(len(self.pipeline.name) + 1 + len(branch.name) > 128 for branch in self.branches):
            raise ValueError("Combined pipeline and branch names must fit the 128-character limit")
        return self


class ServiceStatus(StrictModel):
    state: Literal["created", "starting", "running", "draining", "closed", "failed"]
    revision: int = Field(ge=1)
    completed: int = Field(default=0, ge=0)
    failed: int = Field(default=0, ge=0)
    rejected: int = Field(default=0, ge=0)
    pending: int = Field(default=0, ge=0)
    worker_pids: list[int] = Field(default_factory=list)
    error: ProcessingError | None = None


class RunReport(StrictModel):
    completed: int = Field(default=0, ge=0)
    failed: int = Field(default=0, ge=0)
    rejected: int = Field(default=0, ge=0)
    abandoned: int = Field(default=0, ge=0)
    shutdown: ShutdownReport
