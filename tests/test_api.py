# SPDX-FileCopyrightText: 2026 StreamForge contributors
# SPDX-License-Identifier: Apache-2.0

from fastapi.testclient import TestClient

from streamforge.api import ApiSettings, create_app
from streamforge.image import image_pipeline
from streamforge.models import ImageData, ImagePayload, PipelineConfig, StageConfig, Task
from streamforge.pipeline import Pipeline, Stage
from tests.support import FailNegative, Number, Sleep, SleepParameters


def test_http_image_request_and_openapi_use_concrete_nested_models():
    app = create_app(image_pipeline(tags=["checked"]))
    image = ImageData.from_bytes(b"image-bytes", media_type="image/png")
    with TestClient(app) as client:
        response = client.post(
            "/tasks", json={"task_id": "one", "payload": {"image": image.model_dump()}}
        )
        assert response.status_code == 200
        assert response.json()["payload"]["tags"] == ["checked"]
        assert response.json()["payload"]["image"]["content_base64"] == "aW1hZ2UtYnl0ZXM="
        assert client.get("/health").json()["state"] == "running"
        schema = client.get("/openapi.json").json()
        assert "ImageData" in schema["components"]["schemas"]


def test_http_rejects_extra_fields_and_numeric_coercion_before_submission():
    pipeline = Pipeline(
        config=PipelineConfig(),
        payload_model=Number,
        stages=[Stage(StageConfig(name="check"), FailNegative)],
    )
    with TestClient(create_app(pipeline)) as client:
        for body in [
            {"payload": {"value": "3"}},
            {"payload": {"value": 3, "extra": 0}},
            {"payload": {"value": 3}, "extra": 0},
        ]:
            assert client.post("/tasks", json=body).status_code == 422
        assert pipeline.pending_count == 0
        response = client.post("/tasks", json={"payload": {"value": -1}})
        assert response.status_code == 200
        assert response.json()["status"] == "failed"


def test_http_timeout_discards_unclaimed_result_and_releases_capacity():
    pipeline = Pipeline(
        config=PipelineConfig(max_pending=1),
        payload_model=Number,
        stages=[Stage(StageConfig(name="slow"), Sleep, SleepParameters(seconds=0.1))],
    )
    settings = ApiSettings(submit_timeout=0.1, result_timeout=0.01)
    with TestClient(create_app(pipeline, settings)) as client:
        assert client.post("/tasks", json={"payload": {"value": 1}}).status_code == 504
        assert pipeline.pending_count == 0
        assert client.post("/tasks", json={"payload": {"value": 2}}).status_code == 504
        assert pipeline.pending_count == 0


def test_http_reports_capacity_overload_without_discarding_an_existing_task():
    pipeline = image_pipeline(config=PipelineConfig(max_pending=1))
    with TestClient(create_app(pipeline, ApiSettings(submit_timeout=0.02))) as client:
        existing = Task[ImagePayload](
            task_id="held",
            payload=ImagePayload(
                image=ImageData.from_bytes(b"image", media_type="image/png"),
            ),
        )
        pipeline.submit(existing)
        body = existing.model_dump()
        assert client.post("/tasks", json=body).status_code == 409
        body["task_id"] = "next"
        assert client.post("/tasks", json=body).status_code == 429
        assert pipeline.result("held", timeout=10.0).status == "succeeded"
