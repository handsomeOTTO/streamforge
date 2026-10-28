# SPDX-FileCopyrightText: 2026 StreamForge contributors
# SPDX-License-Identifier: Apache-2.0

"""Run after installing the package: python examples/image_pipeline.py."""

import base64

from streamforge.image import image_pipeline
from streamforge.models import ImageData, ImagePayload, Task

# A synthetic one-pixel PNG for byte-preserving pipeline examples.
PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGP4//8/AAX+Av4N70a4AAAAAElFTkSuQmCC"
)


def main():
    with image_pipeline(workers=2, tags=["received", "processed"]) as pipeline:
        tasks = (
            Task[ImagePayload](
                task_id=f"frame-{index}",
                payload=ImagePayload(image=ImageData.from_bytes(PNG, media_type="image/png")),
            )
            for index in range(6)
        )
        for result in pipeline.map(tasks):
            assert result.payload.image.to_bytes() == PNG
            print(result.model_dump_json())


if __name__ == "__main__":
    main()
