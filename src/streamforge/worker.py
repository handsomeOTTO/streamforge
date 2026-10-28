# SPDX-FileCopyrightText: 2026 StreamForge contributors
# SPDX-License-Identifier: Apache-2.0

"""The single queue-consumption loop used by every processing stage."""

import queue

from .models import ProcessingError, WireTask, WorkerEvent
from .processor import ProcessorContext


def run_worker(
    name,
    processor_type,
    parameters,
    payload_model,
    operators,
    input_queue,
    output_queue,
    control_queue,
    abort,
):
    processor = None
    try:
        processor = processor_type(parameters, ProcessorContext(payload_model, operators))
        processor.setup()
        control_queue.put(WorkerEvent(kind="ready", worker=name).model_dump_json())
        while not abort.is_set():
            try:
                raw = input_queue.get(timeout=0.1)
            except queue.Empty:
                continue
            if raw is None:
                break
            message = WireTask.model_validate_json(raw)
            if message.error is None:
                try:
                    payload = payload_model.model_validate_json(message.payload_json)
                    output = processor.execute(payload, payload_model)
                    message.payload_json = output.model_dump_json()
                except Exception as exc:
                    message.error = ProcessingError(
                        stage=name.rsplit(":", 1)[0],
                        error_type=type(exc).__name__,
                        message=str(exc),
                    )
            encoded = message.model_dump_json()
            while not abort.is_set():
                try:
                    output_queue.put(encoded, timeout=0.1)
                    break
                except queue.Full:
                    continue
    except Exception as exc:
        control_queue.put(
            WorkerEvent(
                kind="failed",
                worker=name,
                error=ProcessingError(stage=name, error_type=type(exc).__name__, message=str(exc)),
            ).model_dump_json()
        )
    finally:
        if processor is not None:
            try:
                processor.teardown()
            except Exception as exc:
                control_queue.put(
                    WorkerEvent(
                        kind="failed",
                        worker=name,
                        error=ProcessingError(
                            stage=name, error_type=type(exc).__name__, message=str(exc)
                        ),
                    ).model_dump_json()
                )
