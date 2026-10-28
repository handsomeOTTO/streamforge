# SPDX-FileCopyrightText: 2026 StreamForge contributors
# SPDX-License-Identifier: Apache-2.0

"""Optional HTTP adapter. One application lifespan owns one pipeline."""

from contextlib import asynccontextmanager
from typing import Literal

from fastapi import FastAPI, HTTPException, Response

from .models import Name, PositiveSeconds, StrictModel, Task, TaskResult
from .pipeline import Pipeline


class ApiSettings(StrictModel):
    submit_timeout: PositiveSeconds = 0.1
    result_timeout: PositiveSeconds = 30.0


class Health(StrictModel):
    name: Name
    state: Literal["created", "starting", "running", "stopping", "closed", "failed"]
    pending: int
    worker_pids: list[int]


def create_app(pipeline: Pipeline, settings: ApiSettings | None = None) -> FastAPI:
    settings = settings or ApiSettings()
    task_model = Task[pipeline.payload_model]
    result_model = TaskResult[pipeline.payload_model]

    @asynccontextmanager
    async def lifespan(app):
        pipeline.start()
        try:
            yield
        finally:
            app.state.shutdown_report = pipeline.close()

    app = FastAPI(title="StreamForge", version="0.1.0", lifespan=lifespan)

    @app.get("/health", response_model=Health)
    def health(response: Response):
        state = pipeline.state
        if state != "running":
            response.status_code = 503
        return Health(
            name=pipeline.config.name,
            state=state,
            pending=pipeline.pending_count,
            worker_pids=pipeline.worker_pids,
        )

    @app.post("/tasks", response_model=result_model)
    def process_task(task: task_model):
        try:
            task_id = pipeline.submit(task, timeout=settings.submit_timeout)
        except TimeoutError as exc:
            raise HTTPException(429, "Pipeline capacity is full") from exc
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(503, "Pipeline is unavailable") from exc
        try:
            return pipeline.result(task_id, timeout=settings.result_timeout)
        except TimeoutError as exc:
            pipeline.discard(task_id)
            raise HTTPException(504, "Task result timed out") from exc

    return app
