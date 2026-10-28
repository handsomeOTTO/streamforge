# SPDX-FileCopyrightText: 2026 StreamForge contributors
# SPDX-License-Identifier: Apache-2.0

"""Exercise real Redis image lookup, CVO, aggregation and normal/health publication."""

import argparse
from pathlib import Path
from uuid import uuid4

from streamforge.application.configuration import load_configuration
from streamforge.application.service import TaskService
from streamforge.messages import ImageReference, ResultMessage, TaskMessage
from streamforge.models import ImagePayload, Task
from streamforge.transports.redis.models import RedisQueues
from streamforge.transports.redis.transport import RedisTransport


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(Path(__file__).with_name("redis.json")))
    arguments = parser.parse_args()
    settings = load_configuration(Path(arguments.config).read_text(encoding="utf-8-sig"))
    prefix = f"streamforge-demo:{uuid4().hex}"
    settings.queues = RedisQueues(
        input=f"{prefix}:tasks",
        processing=f"{prefix}:processing",
        output=f"{prefix}:results",
        rejected=f"{prefix}:rejected",
        health_output=f"{prefix}:health",
    )
    sample = Task[ImagePayload].model_validate_json(
        Path(__file__).with_name("images.jsonl").read_text(encoding="utf-8").strip()
    )
    owned = list(settings.queues.model_dump().values())
    with RedisTransport(settings) as transport:
        try:
            for index in range(3):
                key = f"{prefix}:image:{index}"
                owned.append(key)
                transport.client.set(key, sample.payload.image.content_base64)
                task = TaskMessage(
                    task_id=f"demo-{index}",
                    image=ImageReference(key=key, media_type="image/png"),
                    kind="health" if index == 2 else "inspection",
                    targets=[settings.branches[0].name] if index == 1 else [],
                )
                transport.client.rpush(settings.queues.input, task.model_dump_json())
            report = TaskService(settings).run(max_messages=3)
            assert report.completed == 3 and report.failed == report.rejected == 0
            results = [
                ResultMessage.model_validate_json(raw)
                for queue in (
                    settings.queues.output,
                    settings.queues.health_output,
                )
                for raw in transport.output_client.lrange(queue, 0, -1)
            ]
            assert len(results) == 3
            assert all(result.payload.image == sample.payload.image for result in results)
            assert transport.processing_count == 0
            for result in results:
                print(result.model_dump_json())
            print(report.model_dump_json())
        finally:
            transport.client.delete(*owned)
            transport.output_client.delete(settings.queues.output, settings.queues.health_output)


if __name__ == "__main__":
    main()
