# SPDX-FileCopyrightText: 2026 StreamForge contributors
# SPDX-License-Identifier: Apache-2.0

"""Optional strict HTTP controls for the task-service lifecycle."""

from contextlib import asynccontextmanager
from importlib.metadata import PackageNotFoundError, version

from fastapi import FastAPI, HTTPException, Response

from ..models import StrictModel
from .configuration import configuration_model, load_configuration
from .controller import ControllerStatus, ServiceController


class VersionInfo(StrictModel):
    name: str = "streamforge-core"
    version: str


def create_app(config, *, operators=None, autostart=True, configuration_path=None):
    controller = ServiceController(
        config, operators=operators, configuration_path=configuration_path
    )
    config_model = configuration_model(controller.operators)

    @asynccontextmanager
    async def lifespan(app):
        try:
            if autostart:
                controller.start()
            yield
        finally:
            controller.stop()

    app = FastAPI(title="StreamForge inspection workflow", lifespan=lifespan)
    app.state.controller = controller

    @app.post("/start", response_model=ControllerStatus)
    def start():
        try:
            return controller.start()
        except RuntimeError as exc:
            raise HTTPException(503, str(exc)) from exc

    @app.post("/stop", response_model=ControllerStatus)
    def stop():
        try:
            return controller.stop()
        except RuntimeError as exc:
            raise HTTPException(503, str(exc)) from exc

    @app.get("/health", response_model=ControllerStatus)
    def health(response: Response):
        status = controller.status()
        if status.state != "running":
            response.status_code = 503
        return status

    @app.get("/configuration", response_model=config_model)
    def get_configuration():
        return config_model.model_validate_json(controller.configuration.model_dump_json())

    @app.put("/configuration", response_model=ControllerStatus)
    def configure(configuration: config_model):
        candidate = load_configuration(configuration.model_dump_json(), controller.operators)
        try:
            return controller.configure(candidate)
        except RuntimeError as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.get("/version", response_model=VersionInfo)
    def get_version():
        try:
            installed = version("streamforge-core")
        except PackageNotFoundError:
            installed = "development"
        return VersionInfo(version=installed)

    return app
