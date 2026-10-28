# SPDX-FileCopyrightText: 2026 StreamForge contributors
# SPDX-License-Identifier: Apache-2.0

import time
from pathlib import Path

import pytest
from pydantic import ValidationError

from streamforge.application.configuration import load_configuration
from streamforge.inspection.models import BranchConfig, Finding, InspectionPayload
from streamforge.inspection.workflow import InspectionWorkflow
from streamforge.models import ImageData, PipelineConfig, StrictModel, Task
from streamforge.operators import OperatorRegistry, OperatorSpec


class FindingParameters(StrictModel):
    label: str
    fail: bool = False


class FindingOperator:
    parameters_model = FindingParameters

    def __init__(self, parameters):
        self.parameters = parameters

    def process(self, payload):
        if self.parameters.fail:
            raise ValueError("synthetic branch failure")
        return payload.model_copy(
            update={
                "findings": [
                    *payload.findings,
                    Finding(label=self.parameters.label, confidence=0.8),
                ]
            }
        )


class MarkerParameters(StrictModel):
    path: str


class MarkerOperator:
    parameters_model = MarkerParameters

    def __init__(self, parameters):
        self.path = Path(parameters.path)

    def process(self, payload):
        self.path.write_text("processed", encoding="utf-8")
        return payload


def registry():
    result = OperatorRegistry()
    result.register("fixture", FindingOperator)
    return result


def branch(name, fail=False):
    return BranchConfig(
        name=name,
        operators=[
            OperatorSpec(
                name="fixture",
                parameters=FindingParameters(label=name, fail=fail),
            )
        ],
    )


def task(targets=None):
    return Task[InspectionPayload](
        payload=InspectionPayload(
            image=ImageData.from_bytes(b"source-image", media_type="image/png"),
            targets=targets or [],
        )
    )


def test_task_dispatch_does_not_depend_on_result_retrieval(tmp_path):
    marker = tmp_path / "processed.txt"
    operators = OperatorRegistry()
    operators.register("marker", MarkerOperator)
    configured = BranchConfig(
        name="main",
        operators=[
            OperatorSpec(
                name="marker",
                parameters=MarkerParameters(path=str(marker)),
            )
        ],
    )
    with InspectionWorkflow(
        config=PipelineConfig(), branches=[configured], operators=operators
    ) as workflow:
        workflow.submit(task())
        deadline = time.monotonic() + 2
        while not marker.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert marker.exists()
        assert workflow.receive(timeout=5.0).status == "succeeded"


def test_duplicate_admission_is_rejected_even_when_pending_limit_is_full():
    with InspectionWorkflow(
        config=PipelineConfig(max_pending=1), branches=[branch("a")], operators=registry()
    ) as workflow:
        submitted = task()
        workflow.submit(submitted)
        with pytest.raises(ValueError, match="already pending"):
            workflow.submit(submitted, timeout=0.01)


def test_configurable_branches_aggregate_findings_in_configured_order():
    with InspectionWorkflow(
        config=PipelineConfig(), branches=[branch("a"), branch("b")], operators=registry()
    ) as workflow:
        submitted = task()
        workflow.submit(submitted)
        result = workflow.receive(timeout=10.0)
        assert result.status == "succeeded"
        assert [finding.label for finding in result.payload.findings] == ["a", "b"]
        assert [outcome.branch for outcome in result.inspection.branches] == ["a", "b"]
        assert result.payload.image.to_bytes() == b"source-image"


def test_targeted_dispatch_waits_only_for_selected_branches():
    with InspectionWorkflow(
        config=PipelineConfig(), branches=[branch("a"), branch("b")], operators=registry()
    ) as workflow:
        workflow.submit(task(["b"]))
        result = workflow.receive(timeout=10.0)
        assert [outcome.branch for outcome in result.inspection.branches] == ["b"]
        assert [finding.label for finding in result.payload.findings] == ["b"]


def test_failed_branch_is_accounted_for_without_hanging_other_results():
    with InspectionWorkflow(
        config=PipelineConfig(), branches=[branch("a"), branch("b", True)], operators=registry()
    ) as workflow:
        workflow.submit(task())
        result = workflow.receive(timeout=10.0)
        assert result.status == "failed"
        assert [outcome.status for outcome in result.inspection.branches] == ["succeeded", "failed"]
        assert result.inspection.findings[0].label == "a"
        assert result.payload is None


def test_findings_reject_coerced_numbers_and_extra_fields():
    with pytest.raises(ValidationError):
        Finding.model_validate_json('{"label":"test","confidence":"0.8"}')
    with pytest.raises(ValidationError):
        Finding.model_validate_json('{"label":"test","confidence":0.8,"custom":0}')


def test_configuration_supports_more_than_one_registered_parameter_type():
    from streamforge.application.configuration import default_registry

    operators = default_registry()
    operators.register("fixture", FindingOperator)
    configured = load_configuration(
        '{"branches":[{"name":"main","operators":['
        '{"name":"tag","parameters":{"tag":"ready"}},'
        '{"name":"fixture","parameters":{"label":"one"}}]}]}',
        operators,
    )
    assert isinstance(configured.branches[0].operators[1].parameters, FindingParameters)


def test_empty_registry_rejects_configured_calls_during_schema_validation():
    with pytest.raises(ValidationError):
        load_configuration(
            '{"branches":[{"name":"main","operators":[{"name":"unknown"}]}]}',
            OperatorRegistry(),
        )


def test_configuration_resolves_concrete_registered_operator_parameter_models():
    configured = load_configuration(
        '{"branches":[{"name":"a","operators":[{"name":"fixture",'
        '"parameters":{"label":"one","fail":false}}]}]}',
        registry(),
    )
    assert isinstance(configured.branches[0].operators[0].parameters, FindingParameters)
    with pytest.raises(ValidationError):
        load_configuration(
            '{"branches":[{"name":"a","operators":[{"name":"fixture",'
            '"parameters":{"label":"one","extra":0}}]}]}',
            registry(),
        )
