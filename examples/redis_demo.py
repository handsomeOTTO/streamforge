# SPDX-FileCopyrightText: 2026 StreamForge contributors
# SPDX-License-Identifier: Apache-2.0

"""Run a loopback browser demo backed by the real Redis inspection workflow."""

import argparse
from pathlib import Path
from uuid import uuid4

from fastapi import HTTPException
from fastapi.responses import HTMLResponse

from streamforge.application.api import create_app
from streamforge.application.configuration import load_configuration
from streamforge.application.controller import ControllerStatus
from streamforge.messages import ImageReference, ResultMessage, TaskMessage
from streamforge.models import ImagePayload, Name, StrictModel, Task
from streamforge.transports.redis.models import RedisQueues
from streamforge.transports.redis.transport import RedisTransport


class DemoSubmission(StrictModel):
    task_ids: list[Name]


class DemoSnapshot(StrictModel):
    status: ControllerStatus
    input_db: int
    output_db: int
    results: list[ResultMessage]


def create_demo_app(config):
    app = create_app(config)
    controller = app.state.controller
    sample = Task[ImagePayload].model_validate_json(
        Path(__file__).with_name("images.jsonl").read_text(encoding="utf-8").strip()
    )

    @app.get("/", response_class=HTMLResponse, include_in_schema=False)
    def index():
        return Path(__file__).with_name("redis_demo.html").read_text(encoding="utf-8")

    @app.post("/demo/tasks", response_model=DemoSubmission)
    def send():
        if controller.status().state != "running":
            raise HTTPException(503, "Start the workflow before sending demo tasks")
        settings = controller.configuration
        batch = uuid4().hex
        identifiers = []
        with RedisTransport(settings) as transport:
            image_key = f"{settings.queues.input}:image:{batch}"
            transport.client.set(image_key, sample.payload.image.content_base64)
            for index in range(3):
                task = TaskMessage(
                    task_id=f"demo-{batch}-{index}",
                    image=ImageReference(key=image_key, media_type="image/png"),
                    kind="health" if index == 2 else "inspection",
                    tags=["browser-demo"],
                )
                transport.client.rpush(settings.queues.input, task.model_dump_json())
                identifiers.append(task.task_id)
        return DemoSubmission(task_ids=identifiers)

    @app.get("/demo/results", response_model=DemoSnapshot)
    def results():
        settings = controller.configuration
        with RedisTransport(settings) as transport:
            messages = [
                ResultMessage.model_validate_json(raw)
                for queue in (settings.queues.output, settings.queues.health_output)
                for raw in transport.output_client.lrange(queue, -20, -1)
            ]
        return DemoSnapshot(
            status=controller.status(),
            input_db=settings.connection.db,
            output_db=(
                settings.output_db if settings.output_db is not None else settings.connection.db
            ),
            results=messages,
        )

    return app


def main():
    import uvicorn

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(Path(__file__).with_name("redis.json")))
    parser.add_argument("--port", type=int, default=8000)
    arguments = parser.parse_args()
    settings = load_configuration(Path(arguments.config).read_text(encoding="utf-8-sig"))
    prefix = f"streamforge-demo:{uuid4().hex}"
    settings.queues = RedisQueues(
        input=f"{prefix}:tasks",
        processing=f"{prefix}:processing",
        output=f"{prefix}:results",
        rejected=f"{prefix}:rejected",
        health_output=f"{prefix}:health",
    )
    uvicorn.run(create_demo_app(settings), host="127.0.0.1", port=arguments.port)


if __name__ == "__main__":
    main()
