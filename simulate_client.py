# SPDX-FileCopyrightText: 2026 StreamForge contributors
# SPDX-License-Identifier: Apache-2.0

"""Send one image to a running Redis workflow and save its returned image and result."""

import argparse
import math
import sys
import time
from pathlib import Path
from uuid import uuid4

from pydantic import ValidationError
from redis import RedisError

from streamforge.application.configuration import load_configuration
from streamforge.messages import ImageReference, ResultMessage, TaskMessage
from streamforge.models import ImageData, ImagePayload, Task
from streamforge.transports.redis.transport import RedisTransport

_ROOT = Path(__file__).resolve().parent
_MEDIA_TYPES = {
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".bmp": "image/bmp",
    ".tif": "image/tiff",
    ".tiff": "image/tiff",
    ".webp": "image/webp",
}


def read_image(path: Path | None) -> ImageData:
    if path is None:
        sample = Task[ImagePayload].model_validate_json(
            (_ROOT / "examples" / "images.jsonl").read_text(encoding="utf-8").strip()
        )
        return sample.payload.image
    media_type = _MEDIA_TYPES.get(path.suffix.lower())
    if media_type is None:
        raise ValueError("Image extension must be PNG, JPEG, BMP, TIFF or WebP")
    return ImageData.from_bytes(path.read_bytes(), media_type=media_type)


def receive_result(transport: RedisTransport, task_id: str, timeout: float):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for raw in transport.output_client.lrange(transport.queues.output, 0, -1):
            result = ResultMessage.model_validate_json(raw)
            if result.task_id == task_id:
                return raw, result
        time.sleep(0.1)
    raise TimeoutError(f"Timed out waiting for {task_id}; the queued task and image are retained")


def main(argv=None):
    default_configuration = _ROOT / "debug.local.json"
    if not default_configuration.is_file():
        default_configuration = _ROOT / "examples" / "redis.json"
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=default_configuration)
    parser.add_argument("--image", type=Path, help="Use an image file instead of the synthetic PNG")
    parser.add_argument("--output-dir", type=Path, default=_ROOT / "outputs")
    parser.add_argument("--timeout", type=float, default=3600.0)
    arguments = parser.parse_args(argv)
    if not math.isfinite(arguments.timeout) or arguments.timeout <= 0:
        parser.error("--timeout must be a finite positive number of seconds")
    try:
        settings = load_configuration(arguments.config.read_text(encoding="utf-8-sig"))
        image = read_image(arguments.image)
        arguments.output_dir.mkdir(parents=True, exist_ok=True)
        task_id = f"frame-{uuid4().hex}"
        task = TaskMessage(
            task_id=task_id,
            image=ImageReference(
                key=f"{settings.queues.input}:image:{task_id}",
                media_type=image.media_type,
                encoding="raw",
            ),
        )
        with RedisTransport(settings) as transport:
            transport.client.set(task.image.key, image.to_bytes())
            transport.client.rpush(settings.queues.input, task.model_dump_json())
            print(f"Submitted {task_id}. Waiting for the running service...", flush=True)
            raw, result = receive_result(transport, task_id, arguments.timeout)
            result_path = arguments.output_dir / f"{task_id}.json"
            result_path.write_text(result.model_dump_json(indent=2) + "\n", encoding="utf-8")
            if result.payload is not None:
                extension = next(
                    suffix
                    for suffix, media in _MEDIA_TYPES.items()
                    if media == result.payload.image.media_type
                )
                image_path = arguments.output_dir / f"{task_id}{extension}"
                image_path.write_bytes(result.payload.image.to_bytes())
                print(f"Received image: {image_path}")
            transport.output_client.lrem(settings.queues.output, 1, raw)
            print(f"Result JSON: {result_path}")
            print(
                result.model_dump_json(indent=2, exclude={"payload": {"image": {"content_base64"}}})
            )
            return int(result.status != "succeeded")
    except ValidationError:
        print("Configuration, task or result does not match its strict schema", file=sys.stderr)
        return 2
    except RedisError as exc:
        print(f"Redis operation failed ({type(exc).__name__})", file=sys.stderr)
        return 1
    except (OSError, ValueError, TimeoutError) as exc:
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
