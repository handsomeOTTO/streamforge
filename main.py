# SPDX-FileCopyrightText: 2026 StreamForge contributors
# SPDX-License-Identifier: Apache-2.0

"""Start the Redis workflow and HTTP controls directly from an IDE."""

import sys
from pathlib import Path

from streamforge.__main__ import main as run_cli


def main(argv=None):
    root = Path(__file__).resolve().parent
    configuration = root / "debug.local.json"
    if not configuration.is_file():
        configuration = root / "examples" / "redis.json"
    arguments = sys.argv[1:] if argv is None else argv
    return run_cli(["redis-serve", "--config", str(configuration), "--port", "8001", *arguments])


if __name__ == "__main__":
    raise SystemExit(main())
