# SPDX-FileCopyrightText: 2026 StreamForge contributors
# SPDX-License-Identifier: Apache-2.0

import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run_cli(*args, input_text=""):
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT / "src")
    env["PYTHONIOENCODING"] = "utf-8"
    return subprocess.run(
        [sys.executable, "-m", "streamforge", *args],
        input=input_text,
        capture_output=True,
        encoding="utf-8",
        env=env,
        timeout=20,
    )


def test_jsonl_cli_processes_multiple_images_without_external_services():
    request = '{"payload":{"image":{"media_type":"image/png","content_base64":"aW1hZ2U="}}}'
    completed = run_cli(
        "run", "--workers", "2", "--tag", "checked", input_text=request + "\n" + request + "\n"
    )
    assert completed.returncode == 0, completed.stderr
    results = [json.loads(line) for line in completed.stdout.splitlines()]
    assert len(results) == 2
    assert [r["status"] for r in results] == ["succeeded", "succeeded"]
    assert results[0]["payload"]["image"]["content_base64"] == "aW1hZ2U="
    assert results[0]["payload"]["tags"] == ["checked"]


def test_jsonl_cli_rejects_invalid_nested_image():
    completed = run_cli(
        "run",
        input_text='{"payload":{"image":{"media_type":"image/png","content_base64":"bad!"}}}\n',
    )
    assert completed.returncode != 0
    assert completed.stdout == ""
    assert "Base64" in completed.stderr


def test_cli_refuses_to_overwrite_an_existing_output(tmp_path):
    output = tmp_path / "output.jsonl"
    output.write_text("keep", encoding="utf-8")
    completed = run_cli("run", "--output", str(output))
    assert completed.returncode != 0
    assert output.read_text(encoding="utf-8") == "keep"


def test_importing_public_package_starts_no_workers():
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            "import multiprocessing; import streamforge; import streamforge.image; "
            "assert not multiprocessing.active_children()",
        ],
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONPATH": str(ROOT / "src")},
        timeout=10,
    )
    assert completed.returncode == 0, completed.stderr
