"""Where the live fetcher sends its messages (Role 1: Riya Maurya).

KafkaSink  the real path: topic aq_live, key = str(station_id), value = JSON (CONTRACT.md section 2).
FileSink   fallback if Kafka/Docker will not start: JSON-lines files in data/live/inbox/,
           which the stream job reads with Spark's file source (--source files).
PrintSink  --dry-run: print the messages, send nothing.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

from common.config import KAFKA_BOOTSTRAP, KAFKA_TOPIC, PATHS


class KafkaSink:
    def __init__(self, bootstrap: str = KAFKA_BOOTSTRAP, topic: str = KAFKA_TOPIC):
        from confluent_kafka import Producer

        self.topic = topic
        self.bootstrap = bootstrap
        self.failed = 0
        self.producer = Producer({"bootstrap.servers": bootstrap, "acks": "all", "linger.ms": 50,
                                  "message.timeout.ms": 60000})

    def _delivered(self, err, msg) -> None:
        if err is not None:
            self.failed += 1

    def send(self, messages: list[dict]) -> int:
        for m in messages:
            self.producer.produce(self.topic, key=str(m["station_id"]), value=json.dumps(m).encode("utf-8"),
                                  on_delivery=self._delivered)
            self.producer.poll(0)
        left = self.producer.flush(60)
        if left or self.failed:
            raise RuntimeError(
                f"{left + self.failed} message(s) not confirmed by Kafka at {self.bootstrap}. Is Docker running?\n"
                "  docker compose -f infra/docker-compose.yml up -d      (or use --sink files as a fallback)"
            )
        return len(messages)


class FileSink:
    """Each call writes one new file. Written under a hidden name, then renamed,
    so Spark (which ignores names starting with '.') never reads half a file."""

    def __init__(self, folder=None):
        self.folder = folder or PATHS.live_inbox
        self.folder.mkdir(parents=True, exist_ok=True)

    def send(self, messages: list[dict]) -> int:
        if not messages:
            return 0
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        name = f"batch-{stamp}-{uuid.uuid4().hex[:8]}.json"
        tmp = self.folder / f".{name}.tmp"
        with open(tmp, "w") as fh:
            for m in messages:
                fh.write(json.dumps(m) + "\n")
        tmp.rename(self.folder / name)
        return len(messages)


class PrintSink:
    def send(self, messages: list[dict]) -> int:
        for m in messages:
            print(json.dumps(m))
        return len(messages)


def make_sink(kind: str):
    if kind == "kafka":
        return KafkaSink()
    if kind == "files":
        return FileSink()
    return PrintSink()
