<!-- SPDX-License-Identifier: Apache-2.0 -->

# Development and release practices

## Local checks

Create a virtual environment and install `.[api,redis,dev]`, then run:

```text
python -m ruff check .
python -m ruff format --check .
python -m pytest -q
python -m compileall -q src examples
python tools/check_repository.py
python -m build
```

Use `python -m ruff check --fix .` and `python -m ruff format .` to apply the configured style. Tests use real spawned workers, not mocked multiprocessing. Importing the package must never launch workers. Core code depends only on Pydantic; HTTP dependencies remain optional.

Development dependencies include HTTPX for the minimum supported FastAPI/Starlette versions and HTTPX2 for current Starlette releases, which [prefer HTTPX2](https://www.starlette.io/testclient/). Neither client is a core runtime dependency.

The tested Windows/Python 3.14 dependency resolution is recorded in `requirements/constraints-windows-py314.txt`. To reproduce it, add `-c requirements/constraints-windows-py314.txt` to the installation command. CI also tests the declared version ranges without this platform-specific constraint file.

CI covers Linux and Windows with Python 3.11, 3.12, 3.13, and 3.14. It builds distributions and executes the same repository checks. CI is a configuration until it runs on the repository host; local validation does not imply all matrix jobs passed. Third-party CI actions are pinned to immutable commits and updated through review. Workflow permissions are read-only.

Real Redis integration tests are opt-in locally. Set `STREAMFORGE_REDIS_SERVER` to an installed server executable to let pytest start an isolated loopback process, or set `STREAMFORGE_TEST_REDIS_URL` to a dedicated test instance. Tests use unique owned prefixes and never FLUSHDB. Without either setting, server-dependent tests are skipped and must not be reported as passed. The Linux integration job supplies a real Redis service using a pinned image digest and runs `pytest -m redis` plus the complete roundtrip example. No Redis server binary is shipped in this repository or package.

The included workflow targets GitHub Actions conventions. A Gitea installation may use the same commands in a compatible runner, but runner setup, workflow location, and action execution support must be verified on that host. No deployment secrets or machine-specific runner settings are bundled.

## IDE debugging

Install `.[api,redis,dev]` into the project virtual environment and select that interpreter in the IDE. Create an ignored root `debug.local.json` from `examples/redis.json` and set its Redis connection. For breakpoint inspection, use one worker, a long `task_timeout` such as 3600.0 seconds, and a longer `pipeline.start_timeout` such as 300.0 seconds.

1. Debug root `main.py` directly. No module-run configuration or command-line arguments are required. It delegates to the existing Redis HTTP service entry point and autostarts real processing workers. Default HTTP address: `http://127.0.0.1:8001`; controls are documented at `/docs`. Use `--port` to change the HTTP port.
2. Run root `simulate_client.py` separately. It writes image bytes and a strict task to the configured input database, waits for that task's normal result in the configured output database, then saves the image and complete result JSON under `outputs/`. It does not construct a service, create workers or change queue names.
3. Inspect the service breakpoints and resume execution. The client waits up to 3600 seconds by default. Use `--timeout SECONDS`, `--image PATH` or `--output-dir PATH` to change client behavior. Without `--image`, it uses the repository's synthetic PNG fixture.

Both scripts resolve their default configuration relative to their own file location: `debug.local.json` when present, otherwise `examples/redis.json`. Both accept `--config PATH`; ensure they use the same configuration. HTTP configuration updates require the usual stop/update/start sequence. Restart the client with the accepted configuration if queue or database settings change.

VS Code includes two named configurations in `.vscode/launch.json`. Select the project virtual environment, start **StreamForge: Main service**, wait for readiness, then start **StreamForge: Simulate client** as a second debug session. The main configuration enables subprocess debugging. Source breakpoints belong to local editor state and are not stored in `launch.json`; use the table below to recreate them in another workspace. Use Continue (F5) to follow a task across thread and process boundaries.

The Redis CLI uses the familiar `[timestamp][module][line][level]: message` layout with millisecond timestamps. At the default INFO level, each successful image task produces exactly two task records: `↓ redis => input queue => image key` when received, and `↑ redis => output queue => image key` after publication and acknowledgement. Both include the task identifier, process ID and thread name. The output record includes status and total elapsed time, including queue waits and debugger pauses. Image loading, dispatch, processing and detailed branch outcomes are DEBUG records. Startup/shutdown information and warnings/errors remain visible. Queue and image keys are escaped; image bytes, raw messages and credentials are not logged.

The HTTP service writes console logs to stdout. Console colors are enabled for interactive terminals and PyCharm unless `NO_COLOR` is set. Uvicorn startup and access logs share the same formatter. A plain UTF-8 copy is written beside the selected configuration under `logs/streamforge.log`, rotated at local midnight with up to 30 archived files. Logs are excluded from Git and source distributions. Finite Redis CLI commands keep diagnostics on stderr so their stdout JSON reports remain parseable. This setup uses the Python standard library; importing the framework or embedding its application does not install logging handlers.

The simulation client talks directly to Redis, so it does not generate HTTP access logs. Its console and the service console are independent; a cross-database result can arrive before input acknowledgement and the final push log. Default metadata operators finish quickly. To confirm which service processed work, inspect `/health` counters and compare `/configuration` with the client's configuration. Restart the IDE service after source/logging changes to load them.

The client correlates replies by its unique task identifier and leaves unrelated results in Redis. It saves the complete result locally before removing its output entry. Source images stay in Redis for inspection and possible recovery: cross-database results can become visible before input acknowledgement completes. This simulation client does not delete source images automatically; clean up its image keys only after their tasks are fully acknowledged and no recovery is needed. A client timeout does not cancel the task or delete its image; the paused service can still finish after resuming. Returned images are opaque bytes, not evidence of a detection algorithm.

| Breakpoint | Responsibility | Execution context |
| --- | --- | --- |
| `transports/redis/transport.py`: `claim`, `load_image` | Claim task metadata and fetch the source image | Parent process |
| `inspection/workflow.py`: `_pump` | Submit selected processing branches | Coordinator thread |
| `operators.py`: `CVOProcessor.process` | Invoke enabled operators | Spawned worker |
| `image.py`: `TagOperator.process` | Metadata-only example operator | Spawned worker |
| `inspection/workflow.py`: `_aggregate` | Merge branch findings and outcomes | Coordinator thread |
| `application/service.py`: `_complete` | Construct the terminal Redis result | Service thread |
| `transports/redis/transport.py`: `publish` | Publish result and acknowledge the claim | Service thread |

Enable [PyCharm subprocess attachment](https://www.jetbrains.com/help/pycharm/debugger-python.html) or [VS Code subprocess debugging](https://code.visualstudio.com/docs/python/debugging#_subprocess) to hit worker breakpoints. Continue to the next breakpoint across queue boundaries; ordinary stepping does not jump between processes. Short queue-admission deadlines can expire while paused and cause admission retries. Prefer processing and result boundaries for following one task end to end.

## Package layout

| Responsibility | Import |
| --- | --- |
| End-to-end task service | `streamforge.application.service.TaskService` |
| Start/stop and configuration ownership | `streamforge.application.controller.ServiceController` |
| HTTP system controls | `streamforge.application.api.create_app` |
| Strict operator configuration loading | `streamforge.application.configuration.load_configuration` |
| Service settings and reports | `streamforge.application.models` |
| Task/result envelopes and image keys | `streamforge.messages` |
| Redis connection and queue settings | `streamforge.transports.redis.models` |
| Redis I/O | `streamforge.transports.redis.transport.RedisTransport` |

The application assembles the Redis adapter and configured inspection branches. Service logs use `TaskService`; the service thread is named `task-service`. Per-image INFO records contain downward receipt and upward publication arrows. The IDE table above identifies method boundaries for breakpoints.

## Review checklist

- Demonstrate the reported bug or new behavior with meaningful tests.
- Validate configuration and nested data with concrete strict models.
- Check multiple workers, full queues, pending limits, failure propagation, and cleanup for concurrency changes.
- Keep algorithms and private operator structures outside the framework.
- Keep comments and primary documentation in English; update the designated README translation alongside the English version.
- Verify source and dependency provenance; retain required notices.
- Summarize user-visible behavior and tests in the PR.

## Release checklist

1. Choose the version, review API compatibility, and update package metadata and the changelog.
2. Obtain passing hosted CI and independent maintainer review.
3. Confirm source provenance, dependency licenses, package contents, and absence of sensitive data. Keep private development records and local environment files outside distributions.
4. Verify LICENSE and NOTICE are included in both source and wheel distributions. Install the wheel in a clean environment and run the image/JSONL smoke checks outside the source tree.
5. Enable private vulnerability reporting or publish a monitored private contact. Configure the actual repository URLs and maintainers on the host; do not invent accounts or contact addresses.
6. Create an annotated release tag and release notes only after maintainer authorization. Publish through an authenticated, reviewed process; prefer trusted publishing when supported.

No remote repository, tag, public package, or website is created by local development checks. The policies use common open-source conventions, including PEP 517/621 packaging, SPDX licensing metadata, Semantic Versioning, and reviewable CI. They do not claim certification by an external standards body.
