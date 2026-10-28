# SPDX-FileCopyrightText: 2026 StreamForge contributors
# SPDX-License-Identifier: Apache-2.0

import base64

import pytest
from pydantic import ValidationError

from streamforge.models import (
    EmptyParameters,
    ImageData,
    ImagePayload,
    PipelineConfig,
    ProcessingError,
    StageConfig,
    StrictModel,
    Task,
    TaskResult,
)
from streamforge.processor import BaseProcessor


class Value(StrictModel):
    number: int


class AddParameters(StrictModel):
    amount: int


class Add(BaseProcessor[Value, AddParameters]):
    parameters_model = AddParameters

    def process(self, payload: Value) -> Value:
        return Value(number=payload.number + self.parameters.amount)


class InvalidOutput(Add):
    def process(self, payload: Value) -> Value:
        return Value.model_construct(number="invalid")


@pytest.mark.parametrize(
    "raw",
    [
        '{"payload":{"number":"1"}}',
        '{"payload":{"number":true}}',
        '{"payload":{"number":1,"unexpected":0}}',
        '{"payload":{"number":1},"unexpected":0}',
    ],
)
def test_task_rejects_invalid_nested_input(raw):
    with pytest.raises(ValidationError):
        Task[Value].model_validate_json(raw)


@pytest.mark.parametrize(
    "kwargs",
    [{"queue_capacity": 0}, {"max_pending": 0}, {"queue_capacity": "2"}, {"start_timeout": -1.0}],
)
def test_pipeline_rejects_invalid_limits(kwargs):
    with pytest.raises(ValidationError):
        PipelineConfig(**kwargs)


def test_stage_rejects_empty_name_and_zero_workers():
    with pytest.raises(ValidationError):
        StageConfig(name="", workers=1)
    with pytest.raises(ValidationError):
        StageConfig(name="work", workers=0)


def test_image_round_trip_preserves_opaque_bytes():
    content = b"\x89PNG\r\n\x1a\nopaque-image-data"
    image = ImageData.from_bytes(content, media_type="image/png")
    task = Task[ImagePayload](payload=ImagePayload(image=image))
    decoded = Task[ImagePayload].model_validate_json(task.model_dump_json())
    assert decoded.payload.image.to_bytes() == content
    assert decoded.payload.image.content_base64 == base64.b64encode(content).decode("ascii")
    assert decoded.payload.tags == []


@pytest.mark.parametrize("encoded", ["", "not base64!", "====", "YWJj\n"])
def test_image_rejects_invalid_base64(encoded):
    with pytest.raises(ValidationError):
        ImageData(media_type="image/png", content_base64=encoded)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"status": "succeeded"},
        {"status": "failed", "payload": Value(number=1)},
        {
            "status": "succeeded",
            "payload": Value(number=1),
            "error": ProcessingError(stage="work", error_type="ValueError", message="bad"),
        },
    ],
)
def test_result_enforces_success_error_invariant(kwargs):
    with pytest.raises(ValidationError):
        TaskResult[Value](task_id="one", **kwargs)


def test_processor_validates_parameters_and_returns_typed_output():
    processor = Add(AddParameters(amount=3))
    assert processor.execute(Value(number=4), Value).number == 7
    with pytest.raises((TypeError, ValidationError)):
        Add(EmptyParameters())


def test_processor_revalidates_constructed_invalid_output():
    with pytest.raises(ValidationError):
        InvalidOutput(AddParameters(amount=1)).execute(Value(number=1), Value)
