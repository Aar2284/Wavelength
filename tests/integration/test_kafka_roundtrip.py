import json
import time
import uuid
from datetime import datetime, timezone

import pytest
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaConnectionError

from src.common.config import load_config

RUN_ID = uuid.uuid4().hex
MESSAGES_PER_TOPIC = 3
ASSIGNMENT_TIMEOUT_SEC = 10
COLLECT_TIMEOUT_SEC = 30


@pytest.fixture(scope="module")
def bootstrap() -> str:
    servers = load_config()["kafka"]["bootstrap_servers"]
    try:
        consumer = KafkaConsumer(bootstrap_servers=servers, group_id=None)
        consumer.topics()
        consumer.close()
    except (KafkaConnectionError, OSError) as exc:
        pytest.skip(f"Kafka broker unreachable ({exc}) — run `docker compose up -d`")
    except Exception as exc:
        pytest.skip(f"Kafka broker unreachable ({exc})")
    return servers


def _event_payloads() -> list[dict]:
    now = datetime.now(timezone.utc).isoformat()
    return [
        {
            "runId": RUN_ID,
            "seq": i,
            "userId": f"smoke_user_{i}",
            "trackName": f"Smoke Track {i}",
            "artistName": "Roundtrip Band",
            "timestamp": now,
            "nowPlaying": False,
        }
        for i in range(MESSAGES_PER_TOPIC)
    ]


def _release_payloads() -> list[dict]:
    today = datetime.now(timezone.utc).date().isoformat()
    return [
        {
            "runId": RUN_ID,
            "seq": i,
            "releaseId": f"smoke-release-{i}",
            "title": f"Smoke Release {i}",
            "artist": "Roundtrip Band",
            "releaseDate": today,
            "sourceTag": "smoke",
        }
        for i in range(MESSAGES_PER_TOPIC)
    ]


def _emit(bootstrap: str, topic: str, payloads: list[dict]) -> None:
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    try:
        futures = [
            producer.send(topic, key=f"{RUN_ID}-{p['seq']}", value=p)
            for p in payloads
        ]
        for future in futures:
            future.get(timeout=10)
        producer.flush()
    finally:
        producer.close()


def _wait_for_assignment(consumer: KafkaConsumer) -> None:
    deadline = time.monotonic() + ASSIGNMENT_TIMEOUT_SEC
    while not consumer.assignment() and time.monotonic() < deadline:
        consumer.poll(timeout_ms=250)


def _roundtrip(bootstrap: str, topic: str, payloads: list[dict]) -> dict[int, dict]:
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap,
        group_id=None,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    try:
        consumer.subscribe([topic])
        _wait_for_assignment(consumer)
        if not consumer.assignment():
            raise AssertionError(f"no partition assigned for {topic}")

        start_offsets = consumer.end_offsets(list(consumer.assignment()))
        _emit(bootstrap, topic, payloads)
        for partition, offset in start_offsets.items():
            consumer.seek(partition, offset)

        recovered: dict[int, dict] = {}
        deadline = time.monotonic() + COLLECT_TIMEOUT_SEC
        while len(recovered) < len(payloads) and time.monotonic() < deadline:
            for _partition, messages in consumer.poll(timeout_ms=1000).items():
                for message in messages:
                    value = message.value
                    if isinstance(value, dict) and value.get("runId") == RUN_ID:
                        recovered[value["seq"]] = value
        return recovered
    finally:
        consumer.close()


def test_listening_events_roundtrip(bootstrap: str):
    topic = load_config()["kafka"]["topics"]["listening_events"]
    recovered = _roundtrip(bootstrap, topic, _event_payloads())

    assert sorted(recovered) == list(range(MESSAGES_PER_TOPIC)), (
        f"expected seqs 0..{MESSAGES_PER_TOPIC - 1}, got {sorted(recovered)}"
    )
    for seq, payload in recovered.items():
        assert payload["trackName"] == f"Smoke Track {seq}"
        assert payload["artistName"] == "Roundtrip Band"
        assert payload["userId"] == f"smoke_user_{seq}"
    print(f"[PASS] {topic}: {len(recovered)}/{MESSAGES_PER_TOPIC} round-tripped")


def test_catalog_releases_roundtrip(bootstrap: str):
    topic = load_config()["kafka"]["topics"]["catalog_releases"]
    recovered = _roundtrip(bootstrap, topic, _release_payloads())

    assert sorted(recovered) == list(range(MESSAGES_PER_TOPIC)), (
        f"expected seqs 0..{MESSAGES_PER_TOPIC - 1}, got {sorted(recovered)}"
    )
    for seq, payload in recovered.items():
        assert payload["title"] == f"Smoke Release {seq}"
        assert payload["releaseId"] == f"smoke-release-{seq}"
    print(f"[PASS] {topic}: {len(recovered)}/{MESSAGES_PER_TOPIC} round-tripped")
