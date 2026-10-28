# SPDX-FileCopyrightText: 2026 StreamForge contributors
# SPDX-License-Identifier: Apache-2.0

"""Configurable fan-out/fan-in using the existing shared multiprocessing workers."""

import threading
import time

from ..image import TagParameters
from ..models import PipelineConfig, ProcessingError, ShutdownReport, StageConfig, Task
from ..operators import CVOParameters, CVOProcessor, OperatorSpec
from ..pipeline import Pipeline, Stage
from .models import (
    BranchOutcome,
    InspectionPayload,
    InspectionResult,
    InspectionSummary,
    PendingInspection,
)


class InspectionWorkflow:
    payload_model = InspectionPayload

    def __init__(
        self, *, config: PipelineConfig, branches, operators, workers=1, tags=None, revision=1
    ):
        if not branches or len({branch.name for branch in branches}) != len(branches):
            raise ValueError("Inspection branches must be nonempty and uniquely named")
        self.config = PipelineConfig.model_validate_json(config.model_dump_json())
        self.revision = revision
        self._pipelines = {}
        for branch in branches:
            parameters = CVOParameters(
                operators=[
                    *[
                        OperatorSpec(name="tag", parameters=TagParameters(tag=tag))
                        for tag in tags or []
                    ],
                    *branch.operators,
                ]
            )
            settings = config.model_copy(update={"name": f"{config.name}.{branch.name}"})
            self._pipelines[branch.name] = Pipeline(
                config=settings,
                payload_model=InspectionPayload,
                operators=operators,
                stages=[
                    Stage(
                        StageConfig(name="operators", workers=branch.workers or workers),
                        CVOProcessor,
                        parameters,
                    )
                ],
            )
        self._condition = threading.Condition()
        self._pump_lock = threading.Lock()
        self._pending: dict[str, PendingInspection] = {}
        self._ready: dict[str, InspectionResult] = {}
        self._state = "created"
        self._report = None
        self._failure = None
        self._coordinator = None
        self._stop_coordinator = threading.Event()
        self._wake = threading.Event()
        self._close_lock = threading.Lock()

    @property
    def state(self):
        if self._state == "running" and any(p.state == "failed" for p in self._pipelines.values()):
            return "failed"
        return self._state

    @property
    def worker_pids(self):
        return [pid for pipeline in self._pipelines.values() for pid in pipeline.worker_pids]

    @property
    def pending_count(self):
        with self._condition:
            return len(self._pending)

    def start(self):
        if self._state != "created":
            raise RuntimeError(f"Cannot start inspection workflow in state {self._state}")
        self._state = "starting"
        try:
            for pipeline in self._pipelines.values():
                pipeline.start()
        except BaseException:
            self.close()
            self._state = "failed"
            raise
        self._state = "running"
        self._coordinator = threading.Thread(
            target=self._coordinate, name="inspection-dispatch", daemon=True
        )
        self._coordinator.start()
        return self

    def _coordinate(self):
        try:
            while not self._stop_coordinator.is_set():
                self._wake.wait(0.01)
                self._wake.clear()
                self._pump()
        except Exception as exc:
            self._failure = ProcessingError(
                stage="dispatch", error_type=type(exc).__name__, message=str(exc)
            )
            with self._condition:
                self._state = "failed"
                for task_id, record in self._pending.items():
                    if task_id not in self._ready:
                        for name in record.expected:
                            record.outcomes.setdefault(
                                name,
                                BranchOutcome(
                                    branch=name,
                                    status="failed",
                                    error=self._failure,
                                ),
                            )
                        self._ready[task_id] = self._aggregate(record)
                self._condition.notify_all()

    def submit(self, task, timeout=None):
        if not isinstance(task, Task) or not isinstance(task.payload, InspectionPayload):
            raise TypeError("Inspection admission requires Task[InspectionPayload]")
        validated = Task[InspectionPayload].model_validate_json(
            task.model_dump_json(warnings=False)
        )
        requested = validated.payload.targets
        if len(set(requested)) != len(requested) or any(
            name not in self._pipelines for name in requested
        ):
            raise ValueError("Task targets must identify unique configured branches")
        expected = [name for name in self._pipelines if not requested or name in requested]
        deadline = Pipeline._deadline(timeout)
        with self._condition:
            if self.state != "running":
                raise RuntimeError(f"Inspection workflow is {self.state}")
            if validated.task_id in self._pending:
                raise ValueError("Task identifier is already pending")
            while len(self._pending) >= self.config.max_pending:
                self._condition.wait(Pipeline._remaining(deadline))
                if self.state != "running":
                    raise RuntimeError(f"Inspection workflow is {self.state}")
                if validated.task_id in self._pending:
                    raise ValueError("Task identifier is already pending")
            if self.state != "running":
                raise RuntimeError(f"Inspection workflow is {self.state}")
            if validated.task_id in self._pending:
                raise ValueError("Task identifier is already pending")
            self._pending[validated.task_id] = PendingInspection(task=validated, expected=expected)
        self._wake.set()
        return validated.task_id

    def _aggregate(self, record):
        outcomes = [record.outcomes[name] for name in record.expected]
        findings = [finding for outcome in outcomes for finding in outcome.findings]
        errors = [outcome.error for outcome in outcomes if outcome.error is not None]
        summary = InspectionSummary(revision=self.revision, findings=findings, branches=outcomes)
        payload = (
            None
            if errors
            else record.task.payload.model_copy(
                update={
                    "findings": findings,
                    "tags": list(
                        dict.fromkeys(
                            [
                                *record.task.payload.tags,
                                *[tag for outcome in outcomes for tag in outcome.tags],
                            ]
                        )
                    ),
                }
            )
        )
        return InspectionResult(
            task_id=record.task.task_id,
            status="failed" if errors else "succeeded",
            payload=payload,
            error=errors[0] if errors else None,
            inspection=summary,
        )

    def _pump(self):
        with self._pump_lock:
            with self._condition:
                records = [
                    (task_id, record)
                    for task_id, record in self._pending.items()
                    if task_id not in self._ready
                ]
            for task_id, record in records:
                for name in record.expected:
                    if name in record.submitted or name in record.outcomes:
                        continue
                    pipeline = self._pipelines[name]
                    if pipeline.state != "running":
                        record.outcomes[name] = BranchOutcome(
                            branch=name,
                            status="failed",
                            error=ProcessingError(
                                stage=name,
                                error_type="WorkerUnavailable",
                                message="Branch is unavailable before task admission",
                            ),
                        )
                        continue
                    try:
                        pipeline.submit(record.task, timeout=0.001)
                    except TimeoutError:
                        continue
                    with self._condition:
                        if self._pending.get(task_id) is record:
                            record.submitted.add(name)
                        else:
                            pipeline.discard(task_id)
            for name, pipeline in self._pipelines.items():
                while pipeline.pending_count:
                    try:
                        result = pipeline.receive(timeout=0.001)
                    except TimeoutError:
                        break
                    with self._condition:
                        record = self._pending.get(result.task_id)
                        if record is not None:
                            record.outcomes[name] = BranchOutcome(
                                branch=name,
                                status=result.status,
                                error=result.error,
                                findings=result.payload.findings if result.payload else [],
                                tags=result.payload.tags if result.payload else [],
                            )
            with self._condition:
                for task_id, record in self._pending.items():
                    if task_id not in self._ready and len(record.outcomes) == len(record.expected):
                        self._ready[task_id] = self._aggregate(record)
                self._condition.notify_all()

    def result(self, task_id, timeout=None):
        deadline = Pipeline._deadline(timeout)
        with self._condition:
            while True:
                if task_id not in self._pending:
                    raise KeyError(task_id)
                if task_id in self._ready:
                    return self._take(task_id)
                self._condition.wait(Pipeline._remaining(deadline))

    def receive(self, timeout=None):
        deadline = Pipeline._deadline(timeout)
        with self._condition:
            while True:
                if not self._pending:
                    raise KeyError("No pending inspections")
                task_id = next(iter(self._ready), None)
                if task_id is not None:
                    return self._take(task_id)
                self._condition.wait(Pipeline._remaining(deadline))

    def _take(self, task_id):
        result = self._ready.pop(task_id)
        del self._pending[task_id]
        self._condition.notify_all()
        return result

    def discard(self, task_id):
        with self._pump_lock, self._condition:
            record = self._pending.pop(task_id, None)
            self._ready.pop(task_id, None)
            if record is None:
                return False
            for name in record.submitted:
                self._pipelines[name].discard(task_id)
            self._condition.notify_all()
            return True

    def close(self):
        with self._close_lock:
            return self._close()

    def _close(self):
        if self._report is not None:
            return self._report
        deadline = time.monotonic() + self.config.shutdown_timeout
        with self._condition:
            self._state = "stopping"
            self._condition.notify_all()
            while any(task_id not in self._ready for task_id in self._pending):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self._condition.wait(min(0.01, remaining))
        self._stop_coordinator.set()
        self._wake.set()
        if self._coordinator is not None:
            self._coordinator.join(2.0)
        with self._pump_lock, self._condition:
            for task_id, record in self._pending.items():
                if task_id not in self._ready:
                    if self._failure is None:
                        self._failure = ProcessingError(
                            stage="inspection",
                            error_type="WorkflowShutdown",
                            message="Inspection did not finish before shutdown",
                        )
                    for name in record.expected:
                        if name not in record.outcomes:
                            record.outcomes[name] = BranchOutcome(
                                branch=name,
                                status="failed",
                                error=ProcessingError(
                                    stage=name,
                                    error_type="WorkflowShutdown",
                                    message="Branch did not finish before shutdown",
                                ),
                            )
                        self._pipelines[name].discard(task_id)
                    self._ready[task_id] = self._aggregate(record)
            self._condition.notify_all()
        reports = [pipeline.close() for pipeline in self._pipelines.values()]
        errors = [report.error for report in reports if report.error is not None]
        self._report = ShutdownReport(
            graceful=self._failure is None and all(report.graceful for report in reports),
            forced_workers=[name for report in reports for name in report.forced_workers],
            error=self._failure or (errors[0] if errors else None),
        )
        self._state = "closed"
        return self._report

    def __enter__(self):
        return self.start()

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
