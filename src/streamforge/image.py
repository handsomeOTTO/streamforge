# SPDX-FileCopyrightText: 2026 StreamForge contributors
# SPDX-License-Identifier: Apache-2.0

"""A pure-image default pipeline; only transport and metadata are handled."""

from .models import ImagePayload, PipelineConfig, StageConfig, StrictModel
from .operators import CVOParameters, CVOProcessor, OperatorRegistry, OperatorSpec
from .pipeline import Pipeline, Stage
from .processor import BaseProcessor


class TagParameters(StrictModel):
    tag: str


class TagOperator(BaseProcessor[ImagePayload, TagParameters]):
    """Metadata example; does not read or modify image pixels."""

    parameters_model = TagParameters

    def process(self, payload: ImagePayload) -> ImagePayload:
        return payload.model_copy(update={"tags": [*payload.tags, self.parameters.tag]})


def image_pipeline(
    *, config: PipelineConfig | None = None, workers: int = 1, tags: list[str] | None = None
) -> Pipeline[ImagePayload]:
    registry = OperatorRegistry()
    registry.register("tag", TagOperator)
    parameters = CVOParameters(
        operators=[
            OperatorSpec(name="tag", parameters=TagParameters(tag=tag)) for tag in tags or []
        ]
    )
    return Pipeline(
        config=config or PipelineConfig(),
        payload_model=ImagePayload,
        operators=registry,
        stages=[Stage(StageConfig(name="cvo", workers=workers), CVOProcessor, parameters)],
    )
