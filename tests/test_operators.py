# SPDX-FileCopyrightText: 2026 StreamForge contributors
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

import pytest

from streamforge.models import (
    EmptyParameters,
    ImageData,
    ImagePayload,
    PipelineConfig,
    StageConfig,
    StrictModel,
    Task,
)
from streamforge.operators import CVOParameters, CVOProcessor, OperatorRegistry, OperatorSpec
from streamforge.pipeline import Pipeline, Stage


class TagParameters(StrictModel):
    tag: str


class TagOperator:
    """Intentionally not a framework subclass; private structures stay local."""

    parameters_model = TagParameters

    def __init__(self, parameters: TagParameters):
        self._private = {"custom-layout": (parameters.tag,)}

    def process(self, payload: ImagePayload) -> ImagePayload:
        return ImagePayload(
            image=payload.image, tags=payload.tags + list(self._private["custom-layout"])
        )


class HookParameters(StrictModel):
    path: str


class HookOperator:
    parameters_model = HookParameters

    def __init__(self, parameters):
        self.path = Path(parameters.path)

    def setup(self):
        self.path.write_text("ready", encoding="utf-8")

    def process(self, payload):
        assert self.path.read_text(encoding="utf-8") == "ready"
        return payload

    def teardown(self):
        self.path.write_text("closed", encoding="utf-8")


class InvalidOperator(TagOperator):
    def process(self, payload):
        return {"image": payload.image}


def image_task():
    return Task[ImagePayload](
        payload=ImagePayload(
            image=ImageData.from_bytes(b"opaque-image", media_type="image/png"),
        )
    )


def make_pipeline(registry, specs):
    return Pipeline(
        config=PipelineConfig(),
        payload_model=ImagePayload,
        operators=registry,
        stages=[Stage(StageConfig(name="cvo"), CVOProcessor, CVOParameters(operators=specs))],
    )


def test_cvo_executes_duck_typed_operators_in_order_without_external_data():
    registry = OperatorRegistry()
    registry.register("tag", TagOperator)
    with make_pipeline(
        registry,
        [
            OperatorSpec(name="tag", parameters=TagParameters(tag="first")),
            OperatorSpec(name="tag", parameters=TagParameters(tag="second")),
        ],
    ) as pipeline:
        submitted = image_task()
        pipeline.submit(submitted)
        result = pipeline.receive(timeout=10.0)
        assert result.payload.tags == ["first", "second"]
        assert result.payload.image.to_bytes() == b"opaque-image"


def test_cvo_identity_path_needs_no_registered_algorithm():
    with make_pipeline(OperatorRegistry(), []) as pipeline:
        pipeline.submit(image_task())
        assert pipeline.receive(timeout=10.0).payload.image.to_bytes() == b"opaque-image"


def test_registry_rejects_unknown_operator_or_wrong_parameter_model_before_spawn():
    registry = OperatorRegistry()
    registry.register("tag", TagOperator)
    with pytest.raises(ValueError, match="Unknown operator"):
        make_pipeline(registry, [OperatorSpec(name="missing")])
    with pytest.raises(TypeError, match="TagParameters"):
        make_pipeline(registry, [OperatorSpec(name="tag", parameters=EmptyParameters())])
    with pytest.raises(ValueError, match="already registered"):
        registry.register("tag", TagOperator)


def test_registry_rejects_classes_without_typed_parameters_and_process():
    with pytest.raises(TypeError):
        OperatorRegistry().register("invalid", object)


def test_cvo_runs_optional_operator_lifecycle_hooks(tmp_path):
    registry = OperatorRegistry()
    registry.register("hook", HookOperator)
    marker = tmp_path / "hooks.txt"
    with make_pipeline(
        registry, [OperatorSpec(name="hook", parameters=HookParameters(path=str(marker)))]
    ) as pipeline:
        pipeline.submit(image_task())
        assert pipeline.receive(timeout=10.0).status == "succeeded"
    assert marker.read_text(encoding="utf-8") == "closed"


def test_cvo_rejects_untyped_operator_output_as_failed_task():
    registry = OperatorRegistry()
    registry.register("invalid", InvalidOperator)
    with make_pipeline(
        registry, [OperatorSpec(name="invalid", parameters=TagParameters(tag="unused"))]
    ) as pipeline:
        pipeline.submit(image_task())
        result = pipeline.receive(timeout=10.0)
        assert result.status == "failed"
        assert result.error.error_type == "TypeError"
