# SPDX-FileCopyrightText: 2026 StreamForge contributors
# SPDX-License-Identifier: Apache-2.0

import os
import time
from pathlib import Path

from streamforge.models import EmptyParameters, ImagePayload, StrictModel
from streamforge.processor import BaseProcessor


class Number(StrictModel):
    value: int


class Amount(StrictModel):
    amount: int = 1


class Add(BaseProcessor[Number, Amount]):
    parameters_model = Amount

    def process(self, payload: Number) -> Number:
        return Number(value=payload.value + self.parameters.amount)


class FailNegative(BaseProcessor[Number, EmptyParameters]):
    def process(self, payload: Number) -> Number:
        if payload.value < 0:
            raise ValueError("negative value")
        return payload


class SetupFailure(FailNegative):
    def setup(self):
        raise RuntimeError("cannot initialize")


class AbruptExit(FailNegative):
    def process(self, payload: Number) -> Number:
        os._exit(7)


class SleepParameters(StrictModel):
    seconds: float = 0.2


class Sleep(BaseProcessor[Number, SleepParameters]):
    parameters_model = SleepParameters

    def process(self, payload: Number) -> Number:
        time.sleep(self.parameters.seconds)
        return payload


class MarkerParameters(StrictModel):
    path: str


class Lifecycle(BaseProcessor[Number, MarkerParameters]):
    parameters_model = MarkerParameters

    def setup(self):
        Path(self.parameters.path).write_text(f"setup:{os.getpid()}\n", encoding="utf-8")

    def process(self, payload: Number) -> Number:
        return payload

    def teardown(self):
        with Path(self.parameters.path).open("a", encoding="utf-8") as stream:
            stream.write("teardown\n")


class TeardownFailure(FailNegative):
    def teardown(self):
        raise RuntimeError("teardown failed")


class DelayedExit(FailNegative):
    def process(self, payload: Number) -> Number:
        time.sleep(0.1)
        os._exit(7)


class GateParameters(StrictModel):
    started: str
    release: str


class Gate(BaseProcessor[Number, GateParameters]):
    parameters_model = GateParameters

    def process(self, payload: Number) -> Number:
        if payload.value == 1:
            Path(self.parameters.started).touch()
            while not Path(self.parameters.release).exists():
                time.sleep(0.005)
        return payload


class FailImage(BaseProcessor[ImagePayload, EmptyParameters]):
    def process(self, payload: ImagePayload) -> ImagePayload:
        if "fail" in payload.tags:
            raise ValueError("requested failure")
        return payload


class SleepImage(BaseProcessor[ImagePayload, SleepParameters]):
    parameters_model = SleepParameters

    def process(self, payload: ImagePayload) -> ImagePayload:
        time.sleep(self.parameters.seconds)
        return payload
