# SPDX-FileCopyrightText: 2026 StreamForge contributors
# SPDX-License-Identifier: Apache-2.0

import multiprocessing
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from pydantic import ValidationError

from streamforge.models import PipelineConfig, StageConfig, Task
from streamforge.pipeline import Pipeline, PipelineStartupError, Stage
from tests.support import (
    AbruptExit,
    Add,
    Amount,
    DelayedExit,
    FailNegative,
    Gate,
    GateParameters,
    Lifecycle,
    MarkerParameters,
    Number,
    SetupFailure,
    Sleep,
    SleepParameters,
    TeardownFailure,
)


def make_pipeline(*stages, **limits):
    return Pipeline(
        config=PipelineConfig(**limits),
        payload_model=Number,
        stages=stages or [Stage(StageConfig(name="add"), Add, Amount(amount=2))],
    )


def task(value, task_id=None):
    kwargs = {"task_id": task_id} if task_id is not None else {}
    return Task[Number](payload=Number(value=value), **kwargs)


def assert_workers_gone(pids):
    assert not set(pids).intersection(child.pid for child in multiprocessing.active_children())


def test_spawn_multiple_stages_and_workers_process_a_continuous_stream():
    pipeline = make_pipeline(
        Stage(StageConfig(name="first", workers=2), Add, Amount(amount=1)),
        Stage(StageConfig(name="second", workers=2), Add, Amount(amount=2)),
        queue_capacity=2,
        max_pending=4,
    )
    with pipeline:
        pids = pipeline.worker_pids
        results = list(pipeline.map(task(n) for n in range(12)))
        assert [result.payload.value for result in results] == list(range(3, 15))
        assert all(result.status == "succeeded" for result in results)
        assert len(set(pids)) == 4
    assert_workers_gone(pids)


def test_unclaimed_results_apply_backpressure_until_consumed():
    with make_pipeline(max_pending=1, queue_capacity=1) as pipeline:
        pipeline.submit(task(1, "one"))
        with pytest.raises(TimeoutError):
            pipeline.submit(task(2, "two"), timeout=0.05)
        assert pipeline.result("one", timeout=10.0).payload.value == 3
        pipeline.submit(task(2, "two"))
        assert pipeline.receive(timeout=10.0).payload.value == 4


def test_task_failure_skips_downstream_and_worker_processes_next_task():
    with make_pipeline(
        Stage(StageConfig(name="check"), FailNegative),
        Stage(StageConfig(name="add"), Add, Amount()),
    ) as pipeline:
        pipeline.submit(task(-1, "bad"))
        pipeline.submit(task(2, "good"))
        failed = pipeline.result("bad", timeout=10.0)
        assert failed.status == "failed"
        assert failed.error.stage == "check"
        assert failed.error.error_type == "ValueError"
        assert failed.payload is None
        assert pipeline.result("good", timeout=10.0).payload.value == 3


def test_submit_rejects_duplicate_ids_and_invalid_model_instances():
    with make_pipeline() as pipeline:
        pipeline.submit(task(1, "same"))
        with pytest.raises(ValueError, match="already pending"):
            pipeline.submit(task(2, "same"))
        forged = Task[Number].model_construct(payload=Number.model_construct(value="wrong"))
        with pytest.raises(ValidationError):
            pipeline.submit(forged)
        with pytest.raises(TypeError):
            pipeline.submit({"payload": {"value": 1}})
        assert pipeline.result("same", timeout=10.0).payload.value == 3


def test_initialization_failure_cleans_up_all_spawned_workers():
    before = {child.pid for child in multiprocessing.active_children()}
    pipeline = make_pipeline(Stage(StageConfig(name="broken", workers=2), SetupFailure))
    with pytest.raises(PipelineStartupError, match="cannot initialize"):
        pipeline.start()
    assert pipeline.state == "failed"
    assert {child.pid for child in multiprocessing.active_children()} <= before
    pipeline.close()


def test_abrupt_worker_exit_returns_failure_instead_of_waiting_forever():
    with make_pipeline(Stage(StageConfig(name="crash"), AbruptExit)) as pipeline:
        pipeline.submit(task(1, "lost"))
        result = pipeline.result("lost", timeout=10.0)
        assert result.status == "failed"
        assert result.error.error_type == "WorkerExited"
        assert pipeline.state == "failed"


def test_bounded_shutdown_terminates_stuck_worker_and_reports_pending_failure():
    pipeline = make_pipeline(
        Stage(StageConfig(name="slow"), Sleep, SleepParameters(seconds=30.0)),
        shutdown_timeout=0.2,
    )
    pipeline.start()
    pids = pipeline.worker_pids
    pipeline.submit(task(1, "slow"))
    report = pipeline.close()
    assert report.graceful is False
    assert report.forced_workers
    assert pipeline.result("slow", timeout=1.0).status == "failed"
    assert_workers_gone(pids)


def test_graceful_close_drains_tasks_and_runs_lifecycle_in_child(tmp_path):
    marker = tmp_path / "lifecycle.txt"
    pipeline = make_pipeline(
        Stage(
            StageConfig(name="lifecycle"),
            Lifecycle,
            MarkerParameters(path=str(marker)),
        )
    )
    pipeline.start()
    pids = pipeline.worker_pids
    pipeline.submit(task(5, "last"))
    assert pipeline.close().graceful is True
    assert pipeline.result("last", timeout=1.0).payload.value == 5
    assert marker.read_text(encoding="utf-8").splitlines() == [f"setup:{pids[0]}", "teardown"]
    assert_workers_gone(pids)


def test_discard_releases_capacity_and_ignores_stale_result_for_reused_id():
    with make_pipeline(
        Stage(StageConfig(name="slow"), Sleep, SleepParameters(seconds=0.05)),
        max_pending=1,
    ) as pipeline:
        pipeline.submit(task(1, "same"))
        assert pipeline.discard("same") is True
        pipeline.submit(task(2, "same"), timeout=1.0)
        assert pipeline.result("same", timeout=10.0).payload.value == 2
        assert pipeline.discard("missing") is False


def test_pipeline_validates_stage_parameters_before_creating_processes():
    with pytest.raises((ValidationError, TypeError)):
        make_pipeline(Stage(StageConfig(name="add"), Add))
    with pytest.raises(ValueError, match="unique"):
        make_pipeline(
            Stage(StageConfig(name="same"), Add, Amount()),
            Stage(StageConfig(name="same"), Add, Amount()),
        )


def wait_until(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            raise AssertionError("Synchronization timed out")
        time.sleep(0.005)


def test_failed_submit_cannot_discard_a_replacement_with_the_same_id(tmp_path):
    started, release = tmp_path / "started", tmp_path / "release"
    with (
        make_pipeline(
            Stage(
                StageConfig(name="gate"),
                Gate,
                GateParameters(started=str(started), release=str(release)),
            ),
            queue_capacity=1,
            max_pending=3,
        ) as pipeline,
        ThreadPoolExecutor(max_workers=2) as executor,
    ):
        try:
            pipeline.submit(task(1, "a"))
            wait_until(started.exists)
            pipeline.submit(task(2, "b"))
            old = executor.submit(pipeline.submit, task(3, "reused"), 0.5)
            wait_until(lambda: pipeline.pending_count == 3)
            assert pipeline.discard("reused")
            replacement = executor.submit(pipeline.submit, task(4, "reused"), 3.0)
            wait_until(lambda: pipeline.pending_count == 3)
            with pytest.raises((TimeoutError, RuntimeError)):
                old.result(timeout=2.0)
            assert pipeline.pending_count == 3
            release.touch()
            assert replacement.result(timeout=5.0) == "reused"
            assert pipeline.result("reused", timeout=5.0).payload.value == 4
        finally:
            release.touch()


def test_shutdown_reports_teardown_failure():
    pipeline = make_pipeline(Stage(StageConfig(name="cleanup"), TeardownFailure))
    pipeline.start()
    report = pipeline.close()
    assert report.graceful is False
    assert report.error.error_type == "RuntimeError"
    assert "teardown failed" in report.error.message
    assert_workers_gone(pipeline.worker_pids)


def test_shutdown_reports_worker_crash_while_draining():
    pipeline = make_pipeline(Stage(StageConfig(name="crash"), DelayedExit))
    pipeline.start()
    pipeline.submit(task(1, "lost"))
    report = pipeline.close()
    assert report.graceful is False
    assert report.error.error_type == "WorkerExited"
    result = pipeline.result("lost", timeout=1.0)
    assert result.error.error_type == "WorkerExited"
    assert_workers_gone(pipeline.worker_pids)
