# SPDX-FileCopyrightText: 2026 StreamForge contributors
# SPDX-License-Identifier: Apache-2.0

"""Strictly typed streaming primitives. Importing this package starts no workers."""

from .models import (
    EmptyParameters,
    ImageData,
    ImagePayload,
    PipelineConfig,
    ProcessingError,
    ShutdownReport,
    StageConfig,
    StrictModel,
    Task,
    TaskResult,
)
from .operators import CVOParameters, CVOProcessor, OperatorRegistry, OperatorSpec
from .pipeline import Pipeline, PipelineStartupError, Stage
from .processor import BaseProcessor

__all__ = [
    "BaseProcessor",
    "CVOParameters",
    "CVOProcessor",
    "EmptyParameters",
    "ImageData",
    "ImagePayload",
    "OperatorRegistry",
    "OperatorSpec",
    "Pipeline",
    "PipelineConfig",
    "PipelineStartupError",
    "ProcessingError",
    "ShutdownReport",
    "Stage",
    "StageConfig",
    "StrictModel",
    "Task",
    "TaskResult",
]
