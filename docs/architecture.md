<!-- SPDX-License-Identifier: Apache-2.0 -->

# Architecture and sequence diagrams

These diagrams describe the implemented Redis inspection workflow. Start with the system overview, then the process boundaries and task sequence. The [operational guide](redis-workflow.md) provides run commands; the [workflow guide](workflow.md) maps processing responsibilities to their implementation modules.

Rendered SVG copies are available for viewers without Mermaid support. They are generated from the source blocks below using Mermaid CLI 12.0.0; the framework does not depend on a diagram renderer at runtime.

## System overview

[Open rendered system overview](diagrams/system-overview.svg).

```mermaid
flowchart TB
    Producer["Producer: image bytes and TaskMessage"]
    Consumer["Result consumer"]
    Client["CLI or HTTP control client"]

    subgraph Host["StreamForge application host"]
        Control["ServiceController<br/>Optional HTTP controls"]
        Service["TaskService<br/>Intake and deadlines<br/>Result formatting"]
        Transport["RedisTransport<br/>Claims and image lookup<br/>Result publication"]
        Workflow["InspectionWorkflow<br/>Select targets<br/>Aggregate outcomes"]
        BranchA["Branch A<br/>Pipeline and CVO workers"]
        BranchB["Branch B<br/>Pipeline and CVO workers"]
        Control -.-> Service
        Service <--> Transport
        Service <--> Workflow
        Workflow <--> BranchA
        Workflow <--> BranchB
    end

    subgraph Redis["One standalone Redis server"]
        Input["Input database<br/>Tasks and images<br/>Claims and rejections"]
        Output["Output database<br/>Normal and health results"]
    end

    Producer --> Input
    Client -.-> Control
    Transport <-->|"Claim tasks and GET images"| Input
    Transport -->|"Publish results"| Output
    Output --> Consumer
```

Solid arrows show data exchange; dashed arrows show lifecycle control. Input and output are logical roles: `output_db` can select another database on the same server or reuse `connection.db`. Rejections stay in the input database. One branch is the default; additional branches are configuration, and task `targets` can select a subset.

HTTP mode uses `ServiceController` to run the service on a background thread. The `redis` CLI command runs `TaskService` directly without a controller or HTTP server. JSONL and the in-memory HTTP adapter are separate SDK entry points.

## Processes, threads and operator ownership

[Open rendered process boundaries](diagrams/process-boundaries.svg).

```mermaid
flowchart TB
    subgraph Parent["Parent application process"]
        Service["Service loop<br/>Redis I/O and deadlines"]
        Coordinator["Inspection coordinator thread<br/>Dispatch and aggregate"]
        InputQueue["Branch bounded input queue"]
        OutputQueue["Branch bounded result queue"]
        Collector["Branch result collector thread"]
        Service -->|"submit Task"| Coordinator
        Coordinator --> InputQueue
        OutputQueue --> Collector
        Collector -->|"Pipeline TaskResult"| Coordinator
        Coordinator -->|"InspectionResult"| Service
    end

    subgraph Pool["One branch worker pool: spawned processes"]
        Worker["Shared run_worker loop"]
        CVO["CVOProcessor<br/>Enabled operators in order"]
        Worker --> CVO
    end

    InputQueue -->|"Serialized WireTask"| Worker
    CVO -->|"Serialized result"| OutputQueue
```

The diagram expands one representative branch. Each configured inspection branch owns a one-stage `Pipeline`, a collector thread, bounded queues and a configurable worker pool. Workers in the same pool share its input queue; one task is processed by one worker per selected branch. Branches can run concurrently. The coordinator admits branches incrementally when capacity is available, and continues working independently of result retrieval. It converts pipeline results into typed branch outcomes before aggregation.

Inside each worker, `BaseProcessor` supplies the common execution contract and `CVOProcessor` runs operators sequentially. Operators declare concrete parameter models and a `process` method; optional `setup` and `teardown` hooks own process-local resources. Private intermediate structures belong to operators. There is no framework layer-file loader, per-operator subprocess, or bundled detection algorithm.

The service and workflow bound pending tasks; each pipeline also bounds its queues and pending results. Serialization copies image data across process boundaries. Redis clients remain in the parent process.

## Task processing sequence

[Open rendered task sequence](diagrams/task-sequence.svg).

This sequence assumes the service has started. Pipeline queue handling and collector threads are summarized by the branch-worker participants.

```mermaid
sequenceDiagram
    actor P as Producer
    participant RI as Redis input DB
    participant S as TaskService / RedisTransport
    participant F as InspectionWorkflow coordinator
    participant A as Branch A CVO workers
    participant B as Branch B CVO workers
    participant RO as Redis output DB
    actor C as Consumer

    P->>RI: SET image key with Base64 or raw bytes
    P->>RI: RPUSH strict TaskMessage JSON
    S->>RI: LMOVE input LEFT to processing RIGHT
    RI-->>S: Claimed task metadata
    S->>S: Validate TaskMessage
    S->>RI: GET referenced image key
    RI-->>S: Opaque image bytes
    S->>S: Build Task with InspectionPayload
    S->>F: submit(task)
    F-->>S: Accepted task identifier
    Note over S,F: Intake can continue while accepted work executes

    par Selected branch A
        F->>A: Enqueue serialized task through Pipeline A
        A->>A: Validate payload and run enabled operators
        A-->>F: Typed result via queue and collector
    and Selected branch B
        F->>B: Enqueue serialized task through Pipeline B
        B->>B: Validate payload and run enabled operators
        B-->>F: Typed result via queue and collector
    end

    F->>F: Collect every selected branch outcome
    F->>F: Merge findings in configured branch order
    S->>F: receive() polls for the next ready result
    F-->>S: InspectionResult with branch summary
    S->>S: Format and validate ResultMessage

    alt Output uses the input database
        S->>RI: Lua checks claim, RPUSH result, LREM claim
        RI-->>S: Publication and acknowledgement complete
        Note over RI,RO: Both logical roles use the same database
    else Output uses another database on the same server
        S->>RI: LPOS verifies claim still exists
        S->>RO: RPUSH normal or health result
        RO-->>S: Publication succeeded
        S->>RI: LREM acknowledged claim
    end

    C->>RO: Consume normal or health result
    RO-->>C: Strict ResultMessage JSON
```

Health tasks follow the same operator and aggregation path, then use `health_output`. An empty `targets` list selects all configured branches. The result retains the source-image reference and, on success, the original image bytes; branch-local image transformations are not merged. A branch failure makes the task fail while preserving available findings and all collected branch outcomes in the inspection summary.

The default registry contains only a metadata tag operator. Empty findings do not represent a quality verdict. See the [extension guide](extension-guide.md) for adding trusted external algorithms through strict models.

## Start, stop and configuration sequence

[Open rendered lifecycle sequence](diagrams/lifecycle-sequence.svg).

```mermaid
sequenceDiagram
    actor U as Control client
    participant API as HTTP API
    participant C as ServiceController
    participant S as TaskService thread
    participant F as InspectionWorkflow / Pipelines
    participant W as Spawned workers
    participant R as Redis

    U->>API: POST /start
    API->>C: start()
    C->>S: Construct service and start thread
    S->>R: Connect and require an empty processing list
    S->>F: start()
    F->>W: Spawn workers and initialize processors/operators
    W-->>F: Ready events
    F-->>S: Pipelines and coordinator running
    S-->>C: Signal readiness
    C-->>API: Running status and worker identifiers
    API-->>U: 200 with ControllerStatus

    U->>API: PUT /configuration while running
    API->>C: configure(validated candidate)
    C-->>API: Reject live configuration change
    API-->>U: 409 Conflict

    U->>API: POST /stop
    API->>C: stop()
    C->>S: Set stop event and wait within budget
    S->>S: Stop intake and drain accepted work
    S->>R: Publish completed results and acknowledge claims
    S->>F: close()
    F->>W: Drain and teardown, terminate on timeout
    W-->>F: Exit and cleanup outcomes
    F-->>S: ShutdownReport
    S->>S: Close Redis clients
    Note over S,R: Unfinished claims stay in processing
    S-->>C: Thread exits with report or error
    C-->>API: Stopped or failed ControllerStatus
    API-->>U: ControllerStatus

    U->>API: PUT /configuration while stopped
    API->>API: Validate concrete registered parameter schemas
    API->>C: configure(candidate)
    C->>C: Validate workflow without starting workers
    opt Configuration path provided
        C->>C: Persist configuration with atomic file replacement
    end
    C->>C: Replace configuration and increment revision
    C-->>API: Stopped status with new revision
    API-->>U: ControllerStatus
    U->>API: POST /start
    Note over C,W: A new service and new workers use the accepted revision
```

The CLI control server and browser demo normally autostart the controller during application lifespan. Repeated starts while running return the existing status. `/health` reports state and counters; `/version` reports the installed package version. Startup failures and unfinished shutdowns are exposed as failures. Recovery is required before restarting if the processing list is nonempty.

## Publication failure and explicit recovery

[Open rendered recovery sequence](diagrams/recovery-sequence.svg).

```mermaid
sequenceDiagram
    participant S as TaskService / RedisTransport
    participant RI as Redis input DB
    participant RO as Redis output DB
    actor O as Operator
    participant CLI as redis-recover
    actor C as Result consumer

    Note over S,RO: Cross-database case: output already succeeded
    S->>RO: RPUSH ResultMessage
    RO-->>S: Result stored
    S-xRI: Connection fails before acknowledgement completes
    Note over RI,RO: Claim may remain<br/>Result is already visible
    S->>S: Fail run and close local processing resources
    O->>O: Confirm processing-list owner is stopped
    O->>CLI: Run explicit recovery
    CLI->>RI: LMOVE processing RIGHT to input LEFT until empty
    RI-->>CLI: Unfinished claims restored ahead of newer input
    O->>S: Start a new service run
    S->>RI: Claim recovered task and load image again
    S->>S: Execute selected branches again
    S->>RO: Publish terminal result again
    S->>RI: Acknowledge recovered claim
    C->>RO: Consume results, tolerating repeated task identifiers
```

Delivery is at least once. Same-database publication and acknowledgement use one Lua script; cross-database publication precedes acknowledgement and can duplicate results after interruption. Recovery must run only after the single processing-list owner stops. Operators with side effects and consumers must tolerate replay.

| Other failure | Implemented outcome |
| --- | --- |
| Malformed task or duplicate in-flight identifier | Publish a hashed rejection in the input database and acknowledge that message. |
| Missing or invalid image | Publish a failed result; later tasks can continue. |
| Unknown branch target | Publish a dispatch failure. |
| Operator exception | Produce a failed branch outcome; aggregate selected branch outcomes. |
| Processing deadline exceeded | Discard the local result and publish a timeout failure; an already running operator may continue. |
| Queue at capacity | Retry bounded admission while preserving pending-task accounting; do not fail solely because a queue is full. |
| Result publication fails | Fail the run and retain unacknowledged work for recovery. |

## Implementation map

| Responsibility | Source |
| --- | --- |
| Task, image and processing contracts | [models.py](../src/streamforge/models.py) |
| Task envelopes, image references and published results | [messages.py](../src/streamforge/messages.py) |
| Application configuration, status and run reports | [application/models.py](../src/streamforge/application/models.py) |
| Redis connection, queue settings and database selection | [transports/redis/models.py](../src/streamforge/transports/redis/models.py) |
| Concrete plugin configuration schemas | [application/configuration.py](../src/streamforge/application/configuration.py) |
| Redis claims, image lookup, publication and recovery | [transports/redis/transport.py](../src/streamforge/transports/redis/transport.py) |
| Ingestion, processing deadlines and result formatting | [application/service.py](../src/streamforge/application/service.py) |
| Branch selection and deterministic aggregation | [inspection/workflow.py](../src/streamforge/inspection/workflow.py) |
| Typed findings, branch outcomes and inspection summaries | [inspection/models.py](../src/streamforge/inspection/models.py) |
| Spawn supervision, bounded queues and result collection | [pipeline.py](../src/streamforge/pipeline.py) |
| Shared processing contract and worker loop | [processor.py](../src/streamforge/processor.py), [worker.py](../src/streamforge/worker.py) |
| Operator registration and CVO scheduling | [operators.py](../src/streamforge/operators.py) |
| Lifecycle, configuration revisions and HTTP controls | [application/controller.py](../src/streamforge/application/controller.py), [application/api.py](../src/streamforge/application/api.py) |

The application assembles the current Redis adapter. Its configuration extends `RedisTransportConfig` to preserve the existing flat JSON settings; additional backends are outside this refactor. The adapter depends on shared message and image models, and never imports the application service or controller. Core processing and inspection modules do not depend on Redis. The framework runs on one application host and one standalone Redis server. It does not provide distributed service ownership, general DAG scheduling, cross-server transactions, shared-memory images, or framework-owned layer processing.
