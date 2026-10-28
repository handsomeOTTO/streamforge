# SPDX-FileCopyrightText: 2026 StreamForge contributors
# SPDX-License-Identifier: Apache-2.0

"""JSONL image transport and optional HTTP server."""

import argparse
import signal
import sys
import threading
from contextlib import ExitStack
from pathlib import Path

from pydantic import ValidationError

from .image import image_pipeline
from .log_config import configure_logging
from .models import ImagePayload, Task


def run_jsonl(arguments):
    with ExitStack() as stack:
        source = (
            sys.stdin
            if arguments.input == "-"
            else stack.enter_context(
                open(arguments.input, encoding="utf-8-sig"),
            )
        )
        target = (
            sys.stdout
            if arguments.output == "-"
            else stack.enter_context(
                open(arguments.output, "x", encoding="utf-8"),
            )
        )
        pipeline = stack.enter_context(
            image_pipeline(workers=arguments.workers, tags=arguments.tag)
        )
        failed = False
        for line in source:
            if not line.strip():
                continue
            task = Task[ImagePayload].model_validate_json(line)
            pipeline.submit(task)
            result = pipeline.result(task.task_id)
            target.write(result.model_dump_json() + "\n")
            target.flush()
            failed |= result.status == "failed"
    return int(failed or not pipeline.close().graceful)


def run_redis(arguments):
    from .application.configuration import load_configuration
    from .application.service import TaskService
    from .transports.redis.models import RedisRecoveryReport
    from .transports.redis.transport import RedisTransport

    try:
        config = load_configuration(Path(arguments.config).read_text(encoding="utf-8-sig"))
    except ValidationError as exc:
        raise ValueError("Redis configuration does not match its schema") from exc
    configure_logging(
        Path(arguments.config).resolve().parent / "logs",
        stream=sys.stdout if arguments.command == "redis-serve" else sys.stderr,
    )
    if arguments.command == "redis-recover":
        with RedisTransport(config) as transport:
            recovered = transport.recover()
        print(RedisRecoveryReport(recovered=recovered).model_dump_json())
        return 0
    if arguments.command == "redis-serve":
        import uvicorn

        from .application.api import create_app

        uvicorn.run(
            create_app(config, configuration_path=arguments.config),
            host=arguments.host,
            port=arguments.port,
            log_config=None,
        )
        return 0
    stop = threading.Event()
    previous = {}
    for signum in (signal.SIGINT, signal.SIGTERM):
        previous[signum] = signal.signal(signum, lambda *_: stop.set())
    try:
        report = TaskService(config).run(stop, max_messages=arguments.max_messages)
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)
    print(report.model_dump_json())
    return int(not report.shutdown.graceful)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Typed multiprocessing image streams")
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="Read one typed image task per JSONL line")
    run.add_argument("--input", default="-", help="UTF-8 JSONL file or - for stdin")
    run.add_argument("--output", default="-", help="New UTF-8 JSONL file or - for stdout")
    serve = commands.add_parser("serve", help="Serve the optional HTTP adapter")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)
    redis_run = commands.add_parser("redis", help="Run the complete Redis image workflow")
    redis_run.add_argument("--max-messages", type=int, default=None)
    recover = commands.add_parser(
        "redis-recover", help="Requeue claims while their service is stopped"
    )
    redis_serve = commands.add_parser(
        "redis-serve", help="Serve workflow lifecycle and configuration controls"
    )
    redis_serve.add_argument("--host", default="127.0.0.1")
    redis_serve.add_argument("--port", type=int, default=8000)
    for command in (redis_run, recover, redis_serve):
        command.add_argument(
            "--config", required=True, help="Strict Redis workflow JSON configuration"
        )
    for command in (run, serve):
        command.add_argument("--workers", type=int, default=1, help="CVO processing workers")
        command.add_argument("--tag", action="append", default=[], help="Append transport metadata")
    arguments = parser.parse_args(argv)
    try:
        if arguments.command == "run":
            return run_jsonl(arguments)
        if arguments.command in ("redis", "redis-recover", "redis-serve"):
            from redis import RedisError

            try:
                return run_redis(arguments)
            except RedisError as exc:
                raise RuntimeError(
                    f"Redis operation failed ({type(exc).__name__}); unfinished claims remain "
                    "in the configured processing queue. Inspect the server and queue types "
                    "before recovery."
                ) from exc
        import uvicorn

        from .api import create_app

        uvicorn.run(
            create_app(image_pipeline(workers=arguments.workers, tags=arguments.tag)),
            host=arguments.host,
            port=arguments.port,
        )
        return 0
    except ImportError as exc:
        print(
            f"Optional dependency missing: {exc}. Install streamforge-core[api] or "
            "streamforge-core[redis] for the selected interface.",
            file=sys.stderr,
        )
        return 1
    except (OSError, ValueError, RuntimeError, ValidationError) as exc:
        print(f"StreamForge: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
