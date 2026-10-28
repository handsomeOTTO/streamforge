<!-- SPDX-License-Identifier: Apache-2.0 -->

# Operational Redis inspection workflow

The production-shaped path is Redis input -> image lookup -> branch dispatch -> CVO workers -> typed formatting/aggregation -> Redis output. Core SDK and JSONL remain useful adapters, but do not replace this workflow. See the [workflow guide](workflow.md).

The [architecture and sequence diagrams](architecture.md) show component ownership, worker boundaries, normal task processing, configuration changes and interrupted-publication recovery.

## Run

For a direct IDE service/client workflow, Debug root `main.py`, then run `simulate_client.py` separately to push one image and collect its result. Both use the same configuration and configured queue names. See the [IDE debugging guide](development.md#ide-debugging). The service keeps running between client invocations.

Install the optional Redis integration, and provide a running Redis server version 6.2 or newer:

```powershell
python -m pip install -e '.[api,redis,dev]'
python examples/redis_demo.py --config examples/redis.json
python examples/redis_roundtrip.py --config examples/redis.json
python -m streamforge redis --config examples/redis.json
```

The roundtrip example uses unique disposable keys, writes a synthetic PNG, runs real workers, consumes normal and health results, verifies image bytes, and removes only its own keys. It does not start or install a Redis server. The daemon uses exactly the configured keys and stops on Ctrl+C. Use `--max-messages N` for a finite run.

The browser demo opens a loopback HTTP service at [localhost:8000](http://127.0.0.1:8000). Click **Send 3 demo tasks** to publish two inspection tasks and one health task. Live counters, process identifiers, branch outcomes, image byte counts and raw typed results come from the actual service and Redis queues. Start/stop controls operate the real workers. The demo uses its own randomly prefixed queue/image keys and retains them for inspection; it does not consume production queues or delete unrelated keys. It requires the optional HTTP dependencies and an existing Redis server. It is a local example, not a bundled production UI.

For a real deployment, copy the sample to an ignored `redis.local.json`, change its generic connection/queue settings, and reference credentials through `password_env`. Local/private JSON and environment files are ignored by Git. Do not publish actual connection details in the sample configuration.

`connection.db` selects the input database for tasks, processing claims, rejected messages and image values. Optional `output_db` selects another database on the same server for normal and health results; omit it or set it to `null` to use the input database. The sample demonstrates input database 0 and output database 1. Host, credentials, TLS and timeouts apply to both connections.

For HTTP lifecycle/configuration controls:

```powershell
python -m streamforge redis-serve --config examples/redis.json --host 127.0.0.1 --port 8000
```

| Endpoint | Behavior |
| --- | --- |
| POST /start | Initialize workers and begin consumption; repeated starts are idempotent. |
| POST /stop | Stop intake and drain within the configured budget. |
| GET /health | Return runtime state/counters/workers; 503 when not running. |
| GET /version | Return the installed package version. |
| GET /configuration | Return the strict current configuration. |
| PUT /configuration | Validate and apply while stopped; 409 during a run. |

The CLI control server persists accepted configuration changes atomically to its selected `--config` file. An embedded application can pass `configuration_path` explicitly, or leave it unset for in-memory configuration. Each accepted configuration update increments the result revision. Stop, update, then start: old workers are not mixed with new parameters. These control endpoints require deployment protection outside loopback; no authentication is bundled.

## Producer and consumer

```python
from streamforge.application.configuration import load_configuration
from streamforge.messages import ImageReference, ResultMessage, TaskMessage
from streamforge.transports.redis.transport import RedisTransport

settings = load_configuration(open("examples/redis.json", encoding="utf-8").read())

with RedisTransport(settings) as transport:
    # Store canonical Base64 PNG bytes separately from task metadata.
    transport.client.set("example:image:001", encoded_png)
    task = TaskMessage(
        task_id="frame-001",
        image=ImageReference(key="example:image:001", media_type="image/png"),
    )
    transport.client.rpush(settings.queues.input, task.model_dump_json())
    # A running service consumes the task. A consumer can block for its result.
    _, raw = transport.output_client.blpop(settings.queues.output, timeout=30)
    result = ResultMessage.model_validate_json(raw)
```

The snippet assumes `encoded_png` contains image bytes encoded as a Base64 string. Use `encoding="raw"` on the reference when the Redis value contains raw encoded-image bytes instead. Neither path decodes image pixels. Keep source image values available and immutable until terminal results are consumed; the workflow does not delete or overwrite source images.

Tasks require a unique `task_id`. Optional `targets` lists configured branch names; omit it or use an empty list to select all. `kind="health"` uses the same processing path and the configured health-result queue. Unknown fields, bad encodings and coerced numeric configuration values are rejected.

## Operators, findings and branches

Branches are strict configuration with `name`, optional worker override and a list of enabled operator calls. The default registry contains only a metadata tag operator. It contains no actual CV/AI algorithm.

Plugins declare a concrete parameter model and return `InspectionPayload` with typed `Finding` values. Findings can contain confidence, an optional BoundingBox, polygon points and named measurements. The framework validates these values but does not calculate them. Aggregation preserves the source image, merges findings in configured branch order, and includes all selected `BranchOutcome` values, including errors. Branch-local image transformations are not merged.

Register trusted plugins in an application entry point, then pass that registry to `load_configuration`, `TaskService` or `create_app`. The loader and HTTP/OpenAPI schemas derive parameter types from those registrations. Requests cannot import arbitrary Python code or supply an unchecked parameter dictionary. See [extension-guide.md](extension-guide.md) for the base class and operator protocol.

## Delivery and recovery

The source uses [LMOVE](https://redis.io/docs/latest/commands/lmove/) to move a task atomically from the input list to the processing list. Within one database, a Lua script publishes a result/rejection and removes its processing entry together. With a separate output database, the transport first publishes the result, then acknowledges the input claim. A failure between those two operations can leave both a published result and a recoverable claim; replay can publish a duplicate. Consumers must tolerate repeated task identifiers. Rejected messages stay in the input database with atomic acknowledgement.

Output failures leave the claim in processing; Redis errors stop the service rather than silently losing work. Mutating commands have no transparent client retries.

Malformed task messages go to the rejection queue with a digest and generic error, without echoing arbitrary private input. Missing/invalid images, unknown targets, processor errors and task timeouts become failed results. Completed and rejected messages release their processing entries only after successful publication. Local pending limits apply backpressure; full processing queues do not automatically fail valid tasks.

On stop, intake ends and accepted work drains. Work not finished before the shutdown budget remains in the processing queue. A new run refuses a nonempty processing list. After confirming that its service owner is stopped, run:

```powershell
python -m streamforge redis-recover --config examples/redis.json
```

Recovery restores FIFO order ahead of newer input. It may repeat an operator that started before a crash, so delivery is at least once. Side-effecting operators and consumers must tolerate replay; there is no exactly-once guarantee. Task timeouts abandon local results without promising to interrupt an already running operator. Do not run recovery while a service owns that processing queue.

The input and optional output databases belong to the same configured standalone Redis server. Cross-server transactions and Redis Cluster are outside this transport's scope. Configure ACLs, TLS, persistence, memory limits and external result consumption for the deployment. This package does not embed a Redis server or trim output lists automatically.
