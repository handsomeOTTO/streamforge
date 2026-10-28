# SPDX-FileCopyrightText: 2026 StreamForge contributors
# SPDX-License-Identifier: Apache-2.0

"""Open CVO scheduling; operator-owned resources remain private to operators."""

import inspect
from typing import Protocol

from pydantic import Field, SerializeAsAny

from .models import EmptyParameters, Name, PayloadT, StrictModel
from .processor import BaseProcessor, ProcessorContext


class Operator(Protocol[PayloadT]):
    def process(self, payload: PayloadT) -> PayloadT: ...


class OperatorSpec(StrictModel):
    name: Name
    parameters: SerializeAsAny[StrictModel] = Field(default_factory=EmptyParameters)
    enabled: bool = True


class CVOParameters(StrictModel):
    operators: list[OperatorSpec] = Field(default_factory=list)


class OperatorRegistry:
    def __init__(self):
        self._types: dict[str, type] = {}

    def register(self, name: str, operator_type: type) -> None:
        OperatorSpec(name=name)
        if name in self._types:
            raise ValueError(f"Operator {name} is already registered")
        parameters_model = getattr(operator_type, "parameters_model", None)
        if (
            not isinstance(operator_type, type)
            or inspect.isabstract(operator_type)
            or not isinstance(parameters_model, type)
            or not issubclass(parameters_model, StrictModel)
            or not callable(getattr(operator_type, "process", None))
        ):
            raise TypeError(
                "Operator must declare a StrictModel parameters_model and process method"
            )
        if "<locals>" in operator_type.__qualname__:
            raise TypeError("Spawn operators must be declared at module scope")
        self._types[name] = operator_type

    def validate(self, spec: OperatorSpec) -> OperatorSpec:
        if not isinstance(spec, OperatorSpec):
            raise TypeError("Expected OperatorSpec")
        if spec.name not in self._types:
            raise ValueError(f"Unknown operator: {spec.name}")
        model = self._types[spec.name].parameters_model
        if not isinstance(spec.parameters, model):
            raise TypeError(f"Operator {spec.name} expects {model.__name__}")
        parameters = model.model_validate_json(spec.parameters.model_dump_json(warnings=False))
        return OperatorSpec(name=spec.name, parameters=parameters, enabled=spec.enabled)

    def create(self, spec: OperatorSpec) -> Operator:
        validated = self.validate(spec)
        return self._types[validated.name](validated.parameters)

    def snapshot(self):
        result = OperatorRegistry()
        result._types = self._types.copy()
        return result

    @property
    def parameter_models(self) -> dict[str, type[StrictModel]]:
        return {name: operator.parameters_model for name, operator in self._types.items()}


class CVOProcessor(BaseProcessor[PayloadT, CVOParameters]):
    parameters_model = CVOParameters

    @classmethod
    def validate_configuration(
        cls, parameters: StrictModel, context: ProcessorContext | None = None
    ):
        if not isinstance(parameters, CVOParameters) or not isinstance(parameters.operators, list):
            raise TypeError("CVOProcessor expects CVOParameters")
        if parameters.operators and (context is None or context.operators is None):
            raise ValueError("Configured CVO operators require an OperatorRegistry")
        specs = [context.operators.validate(spec) for spec in parameters.operators]
        return CVOParameters(operators=specs)

    def setup(self):
        self._operators = []
        for spec in self.parameters.operators:
            if spec.enabled:
                operator = self.context.operators.create(spec)
                self._operators.append(operator)
                hook = getattr(operator, "setup", None)
                if callable(hook):
                    hook()

    def process(self, payload: PayloadT) -> PayloadT:
        for operator in self._operators:
            payload = operator.process(payload)
            if not isinstance(payload, self.context.payload_model):
                raise TypeError(f"Operator must return {self.context.payload_model.__name__}")
            payload = self.context.payload_model.model_validate_json(
                payload.model_dump_json(warnings=False)
            )
        return payload

    def teardown(self):
        errors = []
        for operator in reversed(getattr(self, "_operators", [])):
            hook = getattr(operator, "teardown", None)
            if callable(hook):
                try:
                    hook()
                except Exception as exc:
                    errors.append(exc)
        if errors:
            raise RuntimeError("Operator teardown failed") from errors[0]
