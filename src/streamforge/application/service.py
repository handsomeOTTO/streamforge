# SPDX-FileCopyrightText: 2026 StreamForge contributors
# SPDX-License-Identifier: Apache-2.0

"""Bounded task ingestion, processing, formatting, and publication lifecycle."""

import hashlib
import logging
import threading
import time

from pydantic import ValidationError

from ..inspection.models import InspectionPayload
from ..inspection.workflow import InspectionWorkflow
from ..messages import RejectedMessage, ResultMessage, TaskMessage
from ..models import ImagePayload, ProcessingError, StrictModel, Task, TaskResult
from ..transports.redis.transport import RedisTransport
from .configuration import default_registry, load_configuration
from .models import RunReport, ServiceConfig, ServiceStatus

logger = logging.getLogger(__name__)


class _InFlight(StrictModel):
    raw: bytes
    task: TaskMessage
    payload: ImagePayload | None
    started_at: float
    submitted: bool = False


class TaskService:
    def __init__(self, config: ServiceConfig, *, pipeline=None, operators=None, revision=1):
        self.operators = operators.snapshot() if operators is not None else default_registry()
        self.config = load_configuration(config.model_dump_json(), self.operators)
        self.pipeline = (
            pipeline
            if pipeline is not None
            else InspectionWorkflow(
                config=self.config.pipeline,
                branches=self.config.branches,
                operators=self.operators,
                workers=self.config.workers,
                tags=self.config.tags,
                revision=revision,
            )
        )
        if not issubclass(self.pipeline.payload_model, ImagePayload):
            raise TypeError("Image workflow requires an ImagePayload pipeline")
        self.transport = RedisTransport(self.config)
        self.revision = revision
        self._state = "created"
        self._counts = (0, 0, 0, 0)
        self._error = None
        self.ready = threading.Event()

    def snapshot(self) -> ServiceStatus:
        completed, failed, rejected, pending = self._counts
        return ServiceStatus(
            state=self._state,
            revision=self.revision,
            completed=completed,
            failed=failed,
            rejected=rejected,
            pending=pending,
            worker_pids=self.pipeline.worker_pids,
            error=self._error,
        )

    def _publish(self, raw: bytes, result: ResultMessage | RejectedMessage):
        if not self.transport.publish(raw, result):
            raise RuntimeError("Claim disappeared before result publication")

    def _reject(self, raw: bytes, error_type: str):
        digest = hashlib.sha256(raw).hexdigest()
        self._publish(
            raw,
            RejectedMessage(
                message_sha256=digest,
                error=ProcessingError(
                    stage="ingestion",
                    error_type=error_type,
                    message="Input does not match the task message contract",
                ),
            ),
        )
        logger.warning(
            "TaskService reject => message_sha256: %s error_type: %s", digest, error_type
        )

    def _complete(self, record: _InFlight, result: TaskResult[ImagePayload]):
        inspection = getattr(result, "inspection", None)
        self._publish(
            record.raw,
            ResultMessage(
                task_id=result.task_id,
                status=result.status,
                payload=result.payload,
                error=result.error,
                source_image=record.task.image,
                kind=record.task.kind,
                inspection=inspection,
            ),
        )
        logger.debug(
            "TaskService push => task_id: %s status: %s kind: %s cost: %.3fs "
            "branches: %s error_type: %s",
            result.task_id,
            result.status,
            record.task.kind,
            time.monotonic() - record.started_at,
            [(branch.branch, branch.status) for branch in inspection.branches]
            if inspection
            else [],
            result.error.error_type if result.error else None,
        )
        destination = (
            self.config.queues.health_output
            if record.task.kind == "health"
            else self.config.queues.output
        )
        logger.info(
            "↑ redis => %r => %r | task_id: %s status: %s cost: %.3fs",
            destination,
            record.task.image.key,
            result.task_id,
            result.status,
            time.monotonic() - record.started_at,
        )

    def _failure(self, record: _InFlight, error_type: str, message: str):
        self._complete(
            record,
            TaskResult[ImagePayload](
                task_id=record.task.task_id,
                status="failed",
                error=ProcessingError(stage="ingestion", error_type=error_type, message=message),
            ),
        )

    def run(
        self, stop_event: threading.Event | None = None, max_messages: int | None = None
    ) -> RunReport:
        if max_messages is not None and (type(max_messages) is not int or max_messages <= 0):
            raise ValueError("max_messages must be a positive integer or None")
        if self.pipeline.state != "created":
            raise RuntimeError("A task service run requires a new pipeline")
        stop = stop_event if stop_event is not None else threading.Event()
        pending: dict[str, _InFlight] = {}
        claimed = completed = failed = rejected = 0
        drain_deadline = None
        self._state = "starting"
        try:
            self.transport.connect()
            if self.transport.processing_count:
                raise RuntimeError("Processing queue is not empty; recover unfinished work first")
            self.pipeline.start()
            self._state = "running"
            logger.info(
                "TaskService load over => revision: %s workers: %s input_db: %s output_db: %s",
                self.revision,
                self.pipeline.worker_pids,
                self.config.connection.db,
                self.config.output_db
                if self.config.output_db is not None
                else self.config.connection.db,
            )
            self.ready.set()
            while True:
                now = time.monotonic()
                if self.pipeline.state != "running":
                    stop.set()
                if stop.is_set() and drain_deadline is None:
                    drain_deadline = now + self.pipeline.config.shutdown_timeout
                    self._state = "draining"
                if drain_deadline is not None and now >= drain_deadline:
                    break
                intake = not stop.is_set() and (max_messages is None or claimed < max_messages)
                made_progress = False
                if (
                    intake
                    and len(pending) < self.pipeline.config.max_pending
                    and all(item.submitted for item in pending.values())
                ):
                    raw = self.transport.claim()
                    if raw is not None:
                        claimed += 1
                        made_progress = True
                        try:
                            if len(raw) > 65536:
                                raise ValueError("Task envelope exceeds its limit")
                            task = TaskMessage.model_validate_json(raw)
                        except (ValueError, ValidationError):
                            self._reject(raw, "ValidationError")
                            rejected += 1
                        else:
                            if task.task_id in pending:
                                self._reject(raw, "DuplicateTask")
                                rejected += 1
                            else:
                                logger.info(
                                    "↓ redis => %r => %r | task_id: %s",
                                    self.config.queues.input,
                                    task.image.key,
                                    task.task_id,
                                )
                                record = _InFlight(
                                    raw=raw, task=task, payload=None, started_at=time.monotonic()
                                )
                                try:
                                    image = self.transport.load_image(task.image)
                                    values = {"image": image, "tags": task.tags}
                                    if issubclass(self.pipeline.payload_model, InspectionPayload):
                                        values["targets"] = task.targets
                                    elif task.targets:
                                        raise ValueError(
                                            "Custom pipeline does not support branch targets"
                                        )
                                    record.payload = self.pipeline.payload_model(**values)
                                    logger.debug(
                                        "TaskService image loaded => task_id: %s", task.task_id
                                    )
                                except (FileNotFoundError, ValueError):
                                    self._failure(
                                        record,
                                        "InvalidImage",
                                        "Referenced image is missing or invalid",
                                    )
                                    completed += 1
                                    failed += 1
                                else:
                                    pending[task.task_id] = record
                for task_id, record in list(pending.items()):
                    if time.monotonic() - record.started_at >= self.config.task_timeout:
                        if record.submitted:
                            self.pipeline.discard(task_id)
                        self._failure(
                            record, "TaskTimeout", "Task exceeded its processing deadline"
                        )
                        del pending[task_id]
                        completed += 1
                        failed += 1
                        made_progress = True
                    elif not record.submitted and self.pipeline.state == "running":
                        try:
                            self.pipeline.submit(
                                Task[self.pipeline.payload_model](
                                    task_id=task_id,
                                    payload=record.payload,
                                ),
                                timeout=self.config.poll_interval,
                            )
                        except TimeoutError:
                            pass
                        except ValueError:
                            self._failure(
                                record, "DispatchError", "Task targets or admission are invalid"
                            )
                            del pending[task_id]
                            completed += 1
                            failed += 1
                        else:
                            record.submitted = True
                            record.payload = None
                            logger.debug("InspectionWorkflow dispatch => task_id: %s", task_id)
                            made_progress = True
                if any(item.submitted for item in pending.values()):
                    try:
                        result = self.pipeline.receive(timeout=self.config.poll_interval)
                    except TimeoutError:
                        pass
                    else:
                        record = pending[result.task_id]
                        logger.debug(
                            "InspectionWorkflow processing done => task_id: %s status: %s",
                            result.task_id,
                            result.status,
                        )
                        self._complete(record, result)
                        del pending[result.task_id]
                        completed += 1
                        failed += int(result.status == "failed")
                        made_progress = True
                self._counts = (completed, failed, rejected, len(pending))
                if not pending and (
                    stop.is_set() or (max_messages is not None and claimed >= max_messages)
                ):
                    break
                if not made_progress:
                    stop.wait(self.config.poll_interval)
        except BaseException as exc:
            self._state = "failed"
            self._error = ProcessingError(
                stage="redis-workflow", error_type=type(exc).__name__, message=str(exc)
            )
            logger.error("TaskService error => error_type: %s", type(exc).__name__)
            raise
        finally:
            try:
                for task_id, record in pending.items():
                    if record.submitted:
                        self.pipeline.discard(task_id)
                shutdown = self.pipeline.close()
            finally:
                self.transport.close()
                self._counts = (completed, failed, rejected, len(pending))
                self.ready.set()
        if pending and shutdown.graceful:
            shutdown.graceful = False
            shutdown.error = ProcessingError(
                stage="redis-service",
                error_type="UnfinishedClaims",
                message="Unfinished claims require recovery",
            )
        if not shutdown.graceful:
            self._state = "failed"
            self._error = shutdown.error
        else:
            self._state = "closed"
        logger.info(
            "TaskService stop => state: %s completed: %s failed: %s rejected: %s pending: %s",
            self._state,
            completed,
            failed,
            rejected,
            len(pending),
        )
        return RunReport(
            completed=completed,
            failed=failed,
            rejected=rejected,
            abandoned=len(pending),
            shutdown=shutdown,
        )
