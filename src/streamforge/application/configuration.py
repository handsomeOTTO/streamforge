# SPDX-FileCopyrightText: 2026 StreamForge contributors
# SPDX-License-Identifier: Apache-2.0

"""Derive concrete operator parameter schemas from an explicit trusted registry."""

from functools import reduce
from operator import or_
from typing import Annotated, Literal

from pydantic import Field, create_model

from ..image import TagOperator
from ..inspection.models import BranchConfig
from ..models import EmptyParameters, StrictModel
from ..operators import OperatorRegistry, OperatorSpec
from .models import ServiceConfig


def default_registry() -> OperatorRegistry:
    registry = OperatorRegistry()
    registry.register("tag", TagOperator)
    return registry


def configuration_model(operators: OperatorRegistry):
    calls = []
    for index, (name, parameters) in enumerate(operators.parameter_models.items()):
        default = Field(default_factory=EmptyParameters) if parameters is EmptyParameters else ...
        calls.append(
            create_model(
                f"ConfiguredOperator{index}",
                __base__=StrictModel,
                name=(Literal[name], ...),
                parameters=(parameters, default),
                enabled=(bool, True),
            )
        )
    if not calls:
        call_type = OperatorSpec
    elif len(calls) == 1:
        call_type = calls[0]
    else:
        call_type = Annotated[reduce(or_, calls), Field(discriminator="name")]
    branch_type = create_model(
        "ConfiguredBranch",
        __base__=BranchConfig,
        operators=(
            list[call_type],
            Field(default_factory=list, max_length=0 if not calls else None),
        ),
    )
    return create_model(
        "ServiceConfiguration",
        __base__=ServiceConfig,
        branches=(
            list[branch_type],
            Field(default_factory=lambda: [branch_type(name="main")], min_length=1),
        ),
    )


def load_configuration(text: str, operators: OperatorRegistry | None = None) -> ServiceConfig:
    operators = operators if operators is not None else default_registry()
    typed = configuration_model(operators).model_validate_json(text)
    values = {
        name: getattr(typed, name) for name in ServiceConfig.model_fields if name != "branches"
    }
    branches = [
        BranchConfig(
            name=branch.name,
            workers=branch.workers,
            operators=[
                OperatorSpec(name=call.name, parameters=call.parameters, enabled=call.enabled)
                for call in branch.operators
            ],
        )
        for branch in typed.branches
    ]
    return ServiceConfig(**values, branches=branches)
