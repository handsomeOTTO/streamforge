# SPDX-FileCopyrightText: 2026 StreamForge contributors
# SPDX-License-Identifier: Apache-2.0

"""One processing contract shared by stages and convenience operators."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic

from .models import EmptyParameters, ParametersT, PayloadT, StrictModel

if TYPE_CHECKING:
    from .operators import OperatorRegistry


@dataclass(frozen=True)
class ProcessorContext:
    payload_model: type[StrictModel]
    operators: "OperatorRegistry | None" = None


class BaseProcessor(ABC, Generic[PayloadT, ParametersT]):
    """Subclasses only need parameters_model and process for ordinary stages."""

    parameters_model: type[StrictModel] = EmptyParameters

    def __init__(self, parameters: ParametersT, context: ProcessorContext | None = None):
        self.parameters = self.validate_configuration(parameters, context)
        self.context = context

    @classmethod
    def validate_configuration(
        cls, parameters: StrictModel, context: ProcessorContext | None = None
    ):
        if not isinstance(parameters, cls.parameters_model):
            raise TypeError(f"Expected parameters of type {cls.parameters_model.__name__}")
        return cls.parameters_model.model_validate_json(parameters.model_dump_json(warnings=False))

    def setup(self) -> None:
        """Create process-local resources after spawn."""

    @abstractmethod
    def process(self, payload: PayloadT) -> PayloadT:
        """Return a concrete payload model; do not return a dictionary."""

    def execute(self, payload: PayloadT, payload_model: type[PayloadT]) -> PayloadT:
        output = self.process(payload)
        if not isinstance(output, payload_model):
            raise TypeError(f"Processor must return {payload_model.__name__}")
        return payload_model.model_validate_json(output.model_dump_json(warnings=False))

    def teardown(self) -> None:
        """Release process-local resources, including after processing failures."""
