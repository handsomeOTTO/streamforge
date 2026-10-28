# SPDX-FileCopyrightText: 2026 StreamForge contributors
# SPDX-License-Identifier: Apache-2.0

"""Application-owned console formatting and daily file rotation."""

import logging
import os
from io import TextIOWrapper
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import TextIO

_FORMAT = (
    "[%(asctime)s.%(msecs)03d][%(module)s\t][%(lineno)d][%(levelname)s]: "
    "%(message)s | pid: %(process)d thread: %(threadName)s"
)
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


class ConsoleFormatter(logging.Formatter):
    def __init__(self, *, colorize: bool):
        super().__init__(_FORMAT, datefmt=_DATE_FORMAT)
        self.colorize = colorize

    def format(self, record):
        rendered = super().format(record)
        if not self.colorize:
            return rendered
        timestamp = f"[{self.formatTime(record, self.datefmt)}.{int(record.msecs):03d}]"
        location = f"[{record.module}\t][{record.lineno}]"
        color = (
            "\x1b[31m"
            if record.levelno >= logging.ERROR
            else ("\x1b[33m" if record.levelno >= logging.WARNING else "\x1b[37m")
        )
        return (
            f"\x1b[32m{timestamp}\x1b[0m\x1b[36m{location}\x1b[0m"
            f"{color}{rendered[len(timestamp) + len(location) :]}\x1b[0m"
        )


def configure_logging(log_directory: Path, *, stream: TextIO):
    if isinstance(stream, TextIOWrapper):
        stream.reconfigure(encoding="utf-8")
    log_directory.mkdir(parents=True, exist_ok=True)
    console = logging.StreamHandler(stream)
    console.setFormatter(
        ConsoleFormatter(
            colorize=(stream.isatty() or os.environ.get("PYCHARM_HOSTED") == "1")
            and "NO_COLOR" not in os.environ
        )
    )
    file_handler = TimedRotatingFileHandler(
        log_directory / "streamforge.log", when="midnight", backupCount=30, encoding="utf-8"
    )
    file_handler.setFormatter(logging.Formatter(_FORMAT, datefmt=_DATE_FORMAT))
    logging.basicConfig(level=logging.INFO, handlers=[console, file_handler], force=True)
