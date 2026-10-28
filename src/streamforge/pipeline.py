# SPDX-FileCopyrightText: 2026 StreamForge contributors
# SPDX-License-Identifier: Apache-2.0

"""A local spawn-based pipeline with bounded work and explicit ownership."""

import inspect
import multiprocessing
import queue
import threading
import time
from collections import deque
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from typing import Generic
from uuid import uuid4

from .models import (
    EmptyParameters,
    PayloadT,
    PipelineConfig,
    ProcessingError,
    ShutdownReport,
    StageConfig,
    StrictModel,
    Task,
    TaskResult,
    WireTask,
    WorkerEvent,
)
from .processor import BaseProcessor, ProcessorContext
from .worker import run_worker


class PipelineStartupError(RuntimeError):
    pass


@dataclass(frozen=True)
class Stage:
    config: StageConfig
    processor: type[BaseProcessor]
    parameters: StrictModel = field(default_factory=EmptyParameters)


class Pipeline(Generic[PayloadT]):
    def __init__(
        self,
        *,
        config: PipelineConfig,
        payload_model: type[PayloadT],
        stages: Sequence[Stage],
        operators=None,
    ):
        if not issubclass(payload_model, StrictModel):
            raise TypeError("Payload model must inherit StrictModel")
        if not stages:
            raise ValueError("At least one stage is required")
        self.config = PipelineConfig.model_validate_json(config.model_dump_json())
        self.payload_model = payload_model
        self._task_model = Task[payload_model]
        self._result_model = TaskResult[payload_model]
        self._operators = operators.snapshot() if operators is not None else None
        context = ProcessorContext(payload_model, self._operators)
        self._stages = []
        for stage in stages:
            if not isinstance(stage.processor, type) or not issubclass(
                stage.processor, BaseProcessor
            ):
                raise TypeError("Stage processor must inherit BaseProcessor")
            if inspect.isabstract(stage.processor):
                raise TypeError("Stage processor must implement process")
            self._stages.append(
                Stage(
                    StageConfig.model_validate_json(stage.config.model_dump_json()),
                    stage.processor,
                    stage.processor.validate_configuration(stage.parameters, context),
                )
            )
        if len({s.config.name for s in self._stages}) != len(self._stages):
            raise ValueError("Stage names must be unique")
        self._state = "created"
        self._condition = threading.Condition()
        self._ingress_lock = threading.Lock()
        self._close_lock = threading.Lock()
        self._pending: dict[str, str] = {}
        self._results: dict[str, TaskResult[PayloadT]] = {}
        self._processes = []
        self._groups = []
        self._queues = []
        self._control = None
        self._abort = None
        self._collector = None
        self._collector_stop = threading.Event()
        self._failure = None
        self._shutdown_report = None

    @property
    def state(self) -> str:
        with self._condition:
            return self._state

    @property
    def worker_pids(self) -> list[int]:
        return [process.pid for process in self._processes if process.pid is not None]

    @property
    def pending_count(self) -> int:
        with self._condition:
            return len(self._pending)

    def start(self):
        with self._condition:
            if self._state != "created":
                raise RuntimeError(f"Cannot start pipeline in state {self._state}")
            self._state = "starting"
        context = multiprocessing.get_context("spawn")
        try:
            self._abort = context.Event()
            self._control = context.Queue()
            self._queues = [
                context.Queue(self.config.queue_capacity) for _ in range(len(self._stages) + 1)
            ]
            for index, stage in enumerate(self._stages):
                group = []
                self._groups.append(group)
                for worker_index in range(stage.config.workers):
                    name = f"{stage.config.name}:{worker_index}"
                    process = context.Process(
                        name=f"{self.config.name}/{name}",
                        target=run_worker,
                        args=(
                            name,
                            stage.processor,
                            stage.parameters,
                            self.payload_model,
                            self._operators,
                            self._queues[index],
                            self._queues[index + 1],
                            self._control,
                            self._abort,
                        ),
                    )
                    process.start()
                    self._processes.append(process)
                    group.append(process)
            deadline = time.monotonic() + self.config.start_timeout
            ready = set()
            while len(ready) < len(self._processes):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise PipelineStartupError("Worker initialization timed out")
                try:
                    event = WorkerEvent.model_validate_json(
                        self._control.get(timeout=min(0.1, remaining))
                    )
                except queue.Empty:
                    exited = [p.name for p in self._processes if p.exitcode is not None]
                    if exited:
                        raise PipelineStartupError(
                            f"Worker exited during initialization: {exited}"
                        ) from None
                    continue
                if event.kind == "failed":
                    raise PipelineStartupError(f"{event.worker}: {event.error.message}")
                ready.add(event.worker)
            with self._condition:
                self._state = "running"
            self._collector = threading.Thread(
                target=self._collect,
                name=f"{self.config.name}/results",
                daemon=True,
            )
            self._collector.start()
            return self
        except Exception as exc:
            self._failure = ProcessingError(
                stage="startup", error_type=type(exc).__name__, message=str(exc)
            )
            self.close()
            with self._condition:
                self._state = "failed"
            if isinstance(exc, PipelineStartupError):
                raise
            raise PipelineStartupError(str(exc)) from exc

    def submit(self, task: Task[PayloadT], timeout: float | None = None) -> str:
        if not isinstance(task, Task) or not isinstance(task.payload, self.payload_model):
            raise TypeError("Submit requires a Task with the configured payload model")
        validated = self._task_model.model_validate_json(task.model_dump_json(warnings=False))
        dispatch_id = uuid4().hex
        encoded = WireTask(
            dispatch_id=dispatch_id,
            task_id=validated.task_id,
            payload_json=validated.payload.model_dump_json(),
        ).model_dump_json()
        deadline = self._deadline(timeout)
        with self._condition:
            self._require_running()
            if validated.task_id in self._pending:
                raise ValueError(f"Task {validated.task_id} is already pending")
            while len(self._pending) >= self.config.max_pending:
                self._condition.wait(self._remaining(deadline))
                self._require_running()
                if validated.task_id in self._pending:
                    raise ValueError(f"Task {validated.task_id} is already pending")
            self._pending[validated.task_id] = dispatch_id
        try:
            while True:
                remaining = self._remaining(deadline)
                with self._ingress_lock:
                    with self._condition:
                        self._require_running()
                        if self._pending.get(validated.task_id) != dispatch_id:
                            raise RuntimeError("Task submission was discarded or superseded")
                        try:
                            self._queues[0].put_nowait(encoded)
                            return validated.task_id
                        except queue.Full:
                            pass
                self._abort.wait(min(0.01, remaining) if remaining is not None else 0.01)
        except BaseException:
            self._discard(validated.task_id, dispatch_id)
            raise

    def result(self, task_id: str, timeout: float | None = None) -> TaskResult[PayloadT]:
        deadline = self._deadline(timeout)
        with self._condition:
            while True:
                if task_id not in self._pending:
                    raise KeyError(task_id)
                dispatch_id = self._pending[task_id]
                if dispatch_id in self._results:
                    return self._take(dispatch_id)
                self._condition.wait(self._remaining(deadline))

    def receive(self, timeout: float | None = None) -> TaskResult[PayloadT]:
        deadline = self._deadline(timeout)
        with self._condition:
            while not self._results:
                if not self._pending:
                    raise KeyError("No pending tasks")
                self._condition.wait(self._remaining(deadline))
            return self._take(next(iter(self._results)))

    def discard(self, task_id: str) -> bool:
        return self._discard(task_id)

    def _discard(self, task_id: str, expected_dispatch_id: str | None = None) -> bool:
        with self._condition:
            if (
                expected_dispatch_id is not None
                and self._pending.get(task_id) != expected_dispatch_id
            ):
                return False
            dispatch_id = self._pending.pop(task_id, None)
            if dispatch_id is None:
                return False
            self._results.pop(dispatch_id, None)
            self._condition.notify_all()
            return True

    def map(self, tasks: Iterable[Task[PayloadT]]) -> Iterator[TaskResult[PayloadT]]:
        if self.pending_count:
            raise RuntimeError("map requires exclusive use of a pipeline with no pending tasks")
        source = iter(tasks)
        ids = deque()
        outstanding = set()
        exhausted = False
        try:
            while ids or not exhausted:
                while not exhausted and len(ids) < self.config.max_pending:
                    try:
                        task = next(source)
                    except StopIteration:
                        exhausted = True
                        break
                    task_id = self.submit(task)
                    outstanding.add(task_id)
                    ids.append(task_id)
                if ids:
                    task_id = ids.popleft()
                    result = self.result(task_id)
                    outstanding.remove(task_id)
                    yield result
        finally:
            for task_id in outstanding:
                self.discard(task_id)

    def close(self) -> ShutdownReport:
        with self._close_lock:
            if self._shutdown_report is not None:
                return self._shutdown_report
            with self._ingress_lock, self._condition:
                self._state = "stopping"
                self._condition.notify_all()
            deadline = time.monotonic() + self.config.shutdown_timeout
            if self._failure is None:
                for index, group in enumerate(self._groups):
                    for _ in group:
                        try:
                            self._queues[index].put(
                                None, timeout=max(0.001, deadline - time.monotonic())
                            )
                        except queue.Full:
                            break
                    for process in group:
                        process.join(max(0, deadline - time.monotonic()))
                        if process.exitcode not in (None, 0):
                            self._fail(
                                ProcessingError(
                                    stage=process.name,
                                    error_type="WorkerExited",
                                    message=(
                                        "Worker exited during shutdown "
                                        f"with code {process.exitcode}"
                                    ),
                                )
                            )
                    if self._failure is not None or any(p.is_alive() for p in group):
                        break
            if self._abort is not None:
                self._abort.set()
            forced = []
            for process in self._processes:
                if process.is_alive():
                    forced.append(process.name)
                    process.terminate()
                process.join(1.0)
                if process.is_alive():
                    process.kill()
                    process.join(1.0)
            self._collector_stop.set()
            if self._collector is not None:
                self._collector.join(2.0)
            with self._condition:
                if forced and self._failure is None:
                    self._failure = ProcessingError(
                        stage="shutdown",
                        error_type="ShutdownTimeout",
                        message="Workers exceeded the shutdown deadline",
                    )
                for task_id, dispatch_id in self._pending.items():
                    if dispatch_id not in self._results:
                        if self._failure is None:
                            self._failure = ProcessingError(
                                stage="shutdown",
                                error_type="PipelineShutdown",
                                message="Pipeline closed before this task completed",
                            )
                        self._results[dispatch_id] = self._result_model(
                            task_id=task_id,
                            status="failed",
                            error=self._failure,
                        )
                self._state = "closed"
                self._condition.notify_all()
            for channel in [*self._queues, self._control]:
                if channel is not None:
                    channel.cancel_join_thread()
                    channel.close()
            self._shutdown_report = ShutdownReport(
                graceful=not forced and self._failure is None,
                forced_workers=forced,
                error=self._failure,
            )
            return self._shutdown_report

    def _collect(self):
        try:
            while True:
                result_queue_empty = False
                try:
                    raw = self._queues[-1].get(timeout=0.05)
                except queue.Empty:
                    result_queue_empty = True
                else:
                    message = WireTask.model_validate_json(raw)
                    result = self._result_model(
                        task_id=message.task_id,
                        status="failed" if message.error else "succeeded",
                        payload=(
                            None
                            if message.error
                            else self.payload_model.model_validate_json(message.payload_json)
                        ),
                        error=message.error,
                    )
                    with self._condition:
                        if self._pending.get(message.task_id) == message.dispatch_id:
                            self._results.setdefault(message.dispatch_id, result)
                            self._condition.notify_all()
                while True:
                    try:
                        event = WorkerEvent.model_validate_json(self._control.get_nowait())
                    except queue.Empty:
                        break
                    if event.kind == "failed":
                        self._fail(event.error)
                if self.state == "running":
                    for process in self._processes:
                        if process.exitcode is not None:
                            self._fail(
                                ProcessingError(
                                    stage=process.name,
                                    error_type="WorkerExited",
                                    message=(
                                        f"Worker exited unexpectedly with code {process.exitcode}"
                                    ),
                                )
                            )
                            break
                if self._collector_stop.is_set() and result_queue_empty:
                    break
        except Exception as exc:
            self._fail(
                ProcessingError(stage="collector", error_type=type(exc).__name__, message=str(exc))
            )

    def _fail(self, error: ProcessingError):
        with self._condition:
            if self._failure is None:
                self._failure = error
            if self._state == "running":
                self._state = "failed"
            if self._abort is not None:
                self._abort.set()
            for task_id, dispatch_id in self._pending.items():
                self._results.setdefault(
                    dispatch_id,
                    self._result_model(
                        task_id=task_id,
                        status="failed",
                        error=self._failure,
                    ),
                )
            self._condition.notify_all()

    def _take(self, dispatch_id):
        result = self._results.pop(dispatch_id)
        del self._pending[result.task_id]
        self._condition.notify_all()
        return result

    def _require_running(self):
        if self._state != "running":
            raise RuntimeError(f"Pipeline is {self._state}")

    @staticmethod
    def _deadline(timeout):
        if timeout is not None and (timeout <= 0 or not isinstance(timeout, (int, float))):
            raise ValueError("Timeout must be positive or None")
        return None if timeout is None else time.monotonic() + timeout

    @staticmethod
    def _remaining(deadline):
        if deadline is None:
            return None
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Pipeline operation timed out")
        return remaining

    def __enter__(self):
        return self.start()

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
