<!-- SPDX-License-Identifier: Apache-2.0 -->

# Extending stages and operators

## Shared processing base

Define classes at importable module scope, for example in `my_processors.py`:

```python
from streamforge import BaseProcessor, ImagePayload, StrictModel


class MarkParameters(StrictModel):
    label: str


class MarkProcessor(BaseProcessor[ImagePayload, MarkParameters]):
    parameters_model = MarkParameters

    def process(self, payload: ImagePayload) -> ImagePayload:
        return ImagePayload(image=payload.image, tags=[*payload.tags, self.parameters.label])
```

Bind with `Stage(StageConfig(name="mark", workers=2), MarkProcessor, MarkParameters(label="ready"))` and use `payload_model=ImagePayload`. The shared worker loop handles initialization, input/output validation, errors, and cleanup. Override `setup/teardown` to manage resources. Do not create processes or connections at import time; raise errors instead of masking failures with empty successful results.

## Duck-typed operators

Operators do not have to inherit a framework class. The scheduler calls `process` and optional `setup/teardown` hooks. Private structures belong to the operator:

```python
from streamforge import ImagePayload, StrictModel


class LabelParameters(StrictModel):
    label: str


class LabelOperator:
    parameters_model = LabelParameters

    def __init__(self, parameters: LabelParameters):
        self._private_state = {"label": parameters.label}

    def process(self, payload: ImagePayload) -> ImagePayload:
        return ImagePayload(
            image=payload.image,
            tags=[*payload.tags, self._private_state["label"]],
        )
```

No layers, engineering files, fixed layer identifiers, or image algorithms are required. Operator-private intermediate data never changes the public scheduling contract or imposes inputs on other operators.

## Registration

```python
from streamforge import (
    CVOParameters,
    CVOProcessor,
    ImagePayload,
    OperatorRegistry,
    OperatorSpec,
    Pipeline,
    PipelineConfig,
    Stage,
    StageConfig,
)
from my_operators import LabelOperator, LabelParameters


def build_pipeline():
    registry = OperatorRegistry()
    registry.register("label", LabelOperator)
    parameters = CVOParameters(
        operators=[
            OperatorSpec(name="label", parameters=LabelParameters(label="first")),
            OperatorSpec(name="label", parameters=LabelParameters(label="second")),
        ]
    )
    return Pipeline(
        config=PipelineConfig(),
        payload_model=ImagePayload,
        operators=registry,
        stages=[Stage(StageConfig(name="cvo", workers=2), CVOProcessor, parameters)],
    )
```

Parameters are checked against their concrete model before startup and inside workers. Unknown names and wrong parameter models fail before spawning. `enabled=False` disables a call but still requires valid configuration. The registry is snapshotted when constructing the pipeline.

Use concrete models directly or parse their JSON before creating `OperatorSpec`. Do not call `CVOParameters.model_validate_json` on a heterogeneous chain: only the registry knows each plugin's parameter type.

```python
typed_parameters = LabelParameters.model_validate_json('{"label":"ready"}')
call = OperatorSpec(name="label", parameters=typed_parameters)
```

Each worker owns separate operator instances, executed in order with the same concrete payload schema. A failure skips the rest of that task's chain. Cleanup runs in reverse creation order, including after partial initialization failure.

## Inspection workflow payloads

The operational Redis system uses `InspectionPayload`, which extends `ImagePayload` with typed findings and selected branch targets. Operators in that workflow must preserve the concrete payload type. Prefer `payload.model_copy(update=...)` instead of constructing a base `ImagePayload` and dropping inspection fields.

Plugins supply actual detection output through `Finding`, optional `BoundingBox`/polygon points and `Measurement` models. The following illustrates only a result contract, not a detection algorithm:

```python
from streamforge.inspection.models import BoundingBox, Finding

finding = Finding(
    label="example",
    confidence=0.8,
    box=BoundingBox(x=10.0, y=20.0, width=5.0, height=6.0),
)
updated = payload.model_copy(update={"findings": [*payload.findings, finding]})
```

Register an inspection plugin in a trusted application entry point and pass the registry to the configuration loader and Redis service/control application. Multiple registered parameter types generate discriminated concrete schemas. The generic workflow handles background fan-out, selected-branch completion and deterministic aggregation. See [redis-workflow.md](redis-workflow.md).

## HTTP

```python
from streamforge.api import create_app

app = create_app(build_pipeline())
```

The application lifespan starts and closes the pipeline. Request models and OpenAPI reflect its concrete payload type. When using uvicorn module imports, construct configuration and the application at module scope without calling `pipeline.start()`.
