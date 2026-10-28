# SPDX-FileCopyrightText: 2026 StreamForge contributors
# SPDX-License-Identifier: Apache-2.0

"""Explicit workflow ownership and stopped-state configuration changes."""

import os
import tempfile
import threading
from pathlib import Path
from typing import Literal

from pydantic import Field

from ..models import ProcessingError, StrictModel
from .configuration import default_registry, load_configuration
from .service import TaskService


class ControllerStatus(StrictModel):
    state: Literal["stopped", "starting", "running", "draining", "failed"]
    revision: int = Field(ge=1)
    completed: int = Field(default=0, ge=0)
    failed: int = Field(default=0, ge=0)
    rejected: int = Field(default=0, ge=0)
    pending: int = Field(default=0, ge=0)
    worker_pids: list[int] = Field(default_factory=list)
    error: ProcessingError | None = None


class ServiceController:
    def __init__(self, config, *, operators=None, configuration_path=None):
        self.operators = operators.snapshot() if operators is not None else default_registry()
        self._config = load_configuration(config.model_dump_json(), self.operators)
        self.revision = 1
        self._lock = threading.RLock()
        self._thread = None
        self._service = None
        self._stop = None
        self._error = None
        self.report = None
        self._configuration_path = (
            Path(configuration_path).resolve() if configuration_path else None
        )

    def _persist(self, configuration):
        if self._configuration_path is None:
            return
        path = self._configuration_path
        with tempfile.NamedTemporaryFile(
            mode="w",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            encoding="utf-8",
            newline="\n",
            delete=False,
        ) as stream:
            temporary = Path(stream.name)
            try:
                stream.write(configuration.model_dump_json(indent=2) + "\n")
                stream.flush()
                os.fsync(stream.fileno())
            except BaseException:
                stream.close()
                temporary.unlink(missing_ok=True)
                raise
        try:
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)

    @property
    def configuration(self):
        return load_configuration(self._config.model_dump_json(), self.operators)

    def status(self) -> ControllerStatus:
        with self._lock:
            return self._status_snapshot()

    def _status_snapshot(self) -> ControllerStatus:
        if self._service is None:
            return ControllerStatus(state="stopped", revision=self.revision)
        snapshot = self._service.snapshot()
        alive = self._thread is not None and self._thread.is_alive()
        state = snapshot.state
        if state in ("created", "starting"):
            state = "starting" if alive else "stopped"
        elif state == "closed":
            state = "stopped"
        error = self._error or snapshot.error
        if error is not None:
            state = "failed"
        return ControllerStatus(
            state=state,
            revision=self.revision,
            completed=snapshot.completed,
            failed=snapshot.failed,
            rejected=snapshot.rejected,
            pending=snapshot.pending,
            worker_pids=snapshot.worker_pids if alive else [],
            error=error,
        )

    def _execute(self):
        try:
            self.report = self._service.run(self._stop)
        except Exception as exc:
            self._error = ProcessingError(
                stage="controller", error_type=type(exc).__name__, message=str(exc)
            )

    def start(self):
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return self.status()
            self._error = None
            self.report = None
            self._service = TaskService(
                self._config, operators=self.operators, revision=self.revision
            )
            self._stop = threading.Event()
            self._thread = threading.Thread(target=self._execute, name="task-service", daemon=True)
            self._thread.start()
            budget = (
                self._config.pipeline.start_timeout * len(self._config.branches)
                + self._config.connection.connect_timeout
                + self._config.pipeline.shutdown_timeout
                + 2
            )
            if not self._service.ready.wait(budget):
                self._stop.set()
                raise RuntimeError("Workflow initialization timed out")
            status = self.status()
            if status.state != "running":
                raise RuntimeError(
                    status.error.message if status.error else "Workflow did not start"
                )
            return status

    def stop(self):
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                self._stop.set()
                budget = (
                    self._config.pipeline.shutdown_timeout * (len(self._config.branches) + 2)
                    + self._config.connection.socket_timeout * 2
                    + 2
                )
                self._thread.join(budget)
                if self._thread.is_alive():
                    raise RuntimeError("Workflow did not stop within its bounded budget")
            return self.status()

    def configure(self, config):
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                raise RuntimeError("Stop the workflow before changing configuration")
            candidate = load_configuration(config.model_dump_json(), self.operators)
            prepared = TaskService(candidate, operators=self.operators, revision=self.revision + 1)
            prepared.pipeline.close()
            self._persist(candidate)
            self._config = candidate
            self.revision += 1
            self._error = None
            self._service = None
            return self.status()
