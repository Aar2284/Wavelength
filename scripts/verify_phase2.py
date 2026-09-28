import argparse
import json
import os
import shutil
import sys
import time
from collections import Counter
from pathlib import Path

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

from src.common.config import load_config
from src.common.serde import JsonSerializer, Utf8StringSerializer
from src.ingestion import catalog_stream, events_stream, trending_stream
from src.ingestion.events_stream import build_spark
from src.pollers.catalog_poller import (
    RateLimiter,
    build_release_event,
    extract_releases,
    fetch_releases,
    poll_catalog,
)
from src.pollers.events_poller import (
    build_event,
    fetch_recent_tracks,
    make_producer,
    read_seed_users,
    select_new_tracks,
)
from src.pollers.state import SeenStore

MALFORMED = [
    "this-is-not-json",
    json.dumps(
        {
            "userId": "mal-bad-ts",
            "trackName": "T",
            "artistName": "A",
            "timestamp": "not-a-timestamp",
            "nowPlaying": False,
            "source": "verify",
        }
    ),
    json.dumps(
        {
            "trackName": "T",
            "artistName": "A",
            "timestamp": "2020-01-01T00:00:00+00:00",
            "nowPlaying": False,
            "source": "verify",
        }
    ),
    json.dumps(
        {
            "userId": "mal-no-track",
            "artistName": "A",
            "timestamp": "2020-01-01T00:00:00+00:00",
            "nowPlaying": False,
            "source": "verify",
        }
    ),
    json.dumps(
        {
            "userId": "mal-no-artist",
            "trackName": "T",
            "timestamp": "2020-01-01T00:00:00+00:00",
            "nowPlaying": False,
            "source": "verify",
        }
    ),
    json.dumps({"userId": "mal-empty", "timestamp": "2020-01-01T00:00:00+00:00"}),
    "}{",
    json.dumps(
        {
            "userId": None,
            "trackName": None,
            "artistName": None,
            "timestamp": None,
            "nowPlaying": False,
            "source": "verify",
        }
    ),
    json.dumps(
        {
            "userId": "mal-null-ts",
            "trackName": "T",
            "artistName": "A",
            "timestamp": None,
            "nowPlaying": False,
            "source": "verify",
        }
    ),
    '{"userId": "mal-truncated", "trackName": "T"',
    json.dumps(
        {
            "userId": "mal-wrong-type",
            "trackName": 12345,
            "artistName": ["not", "a", "string"],
            "timestamp": "not-a-timestamp",
            "nowPlaying": False,
            "source": "verify",
        }
    ),
    json.dumps(
        {
            "userId": "mal-future",
            "trackName": "T",
            "artistName": "A",
            "timestamp": "9999-99-99T99:99:99+00:00",
            "nowPlaying": False,
            "source": "verify",
        }
    ),
]

WAVE1_FRACTION = 0.4


def log(message):
    print(f"[verify] {message}", flush=True)


def wait_for_broker(cfg, timeout=90):
    from kafka import KafkaConsumer

    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            consumer = KafkaConsumer(bootstrap_servers=cfg["kafka"]["bootstrap_servers"])
            consumer.topics()
            consumer.close()
            return
        except Exception:
            time.sleep(3)
    raise SystemExit("[verify] Kafka broker did not become reachable")


def wipe_paths(cfg=None):
    targets = [
        ROOT / "data" / "checkpoints" / "events",
        ROOT / "data" / "checkpoints" / "trending",
        ROOT / "data" / "checkpoints" / "catalog",
        ROOT / "data" / "events_store",
        ROOT / "data" / "trending_store",
        ROOT / "data" / "catalog_store",
        ROOT / "data" / "state" / "verify_catalog_seen.json",
    ]
    for target in targets:
        if target.is_dir():
            shutil.rmtree(target)
            log(f"wiped {target.relative_to(ROOT)}")
        elif target.exists():
            target.unlink()
            log(f"wiped {target.relative_to(ROOT)}")
    if cfg is not None:
        recreate_topics(cfg)


def recreate_topics(cfg, timeout=90):
    topics = [
        cfg["kafka"]["topics"]["listening_events"],
        cfg["kafka"]["topics"]["catalog_releases"],
    ]
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            admin = KafkaAdminClient(bootstrap_servers=cfg["kafka"]["bootstrap_servers"])
            existing = set(admin.list_topics())
            doomed = [name for name in topics if name in existing]
            if doomed:
                admin.delete_topics(doomed)
                admin.close()
                log(f"deleted topics {doomed}")
                time.sleep(5)
                continue
            admin.create_topics(
                [NewTopic(name, num_partitions=2, replication_factor=1) for name in topics]
            )
            admin.close()
            log(f"recreated topics {topics} empty")
            return
        except Exception as exc:
            log(f"topic recreate step failed ({exc}); retrying")
            time.sleep(5)
    raise SystemExit("[verify] could not recreate Kafka topics")


def collect_events(cfg, api_key):
    settings = cfg["lastfm"]
    topic_events = []
    failures = 0
    for username in read_seed_users(ROOT / settings["user_list"]):
        try:
            tracks = fetch_recent_tracks(settings, api_key, username, cursor=None)
            for uts, track in select_new_tracks(tracks, None):
                event = build_event(username, track)
                if event is not None:
                    topic_events.append((uts, event))
        except Exception:
            failures += 1
    topic_events.sort(key=lambda item: item[0])
    unique = []
    seen_keys = set()
    for uts, event in topic_events:
        key = (event["userId"], event["artistName"], event["trackName"], uts)
        if key not in seen_keys:
            seen_keys.add(key)
            unique.append(event)
    log(f"fetched {len(unique)} unique events ({len(topic_events) - len(unique)} duplicates dropped), {failures} users failed")
    return unique


def retry_call(fn, attempts=6, base_delay=15, label="call"):
    delay = base_delay
    for attempt in range(attempts):
        try:
            return fn()
        except Exception as exc:
            if attempt == attempts - 1:
                raise
            log(f"{label} failed ({exc}); retrying in {delay}s")
            time.sleep(delay)
            delay = min(delay * 2, 120)


def fetch_with_backoff(settings, user_agent, query, limiter, attempts=5):
    delay = 5
    for attempt in range(attempts):
        try:
            return fetch_releases(settings, user_agent, query, limiter=limiter)
        except Exception as exc:
            if attempt == attempts - 1:
                raise
            log(f"MusicBrainz fetch failed ({exc}); retrying in {delay}s")
            time.sleep(delay)
            delay *= 3


def publish_tagged_releases(cfg, user_agent, events, producer, seen):
    settings = cfg["musicbrainz"]
    limiter = RateLimiter(settings["rate_limit_sec"])
    artist_counts = Counter(event["artistName"] for event in events)
    for artist, _ in artist_counts.most_common(5):
        releases = extract_releases(
            fetch_with_backoff(settings, user_agent, f'artist:"{artist}"', limiter)
        )
        tagged = [
            release
            for release in releases
            if release.get("genre") and release["releaseId"] not in seen
        ]
        if tagged:
            for release in tagged:
                producer.send(
                    cfg["kafka"]["topics"]["catalog_releases"],
                    key=release["releaseId"],
                    value=build_release_event(release),
                )
            producer.flush()
            seen.record([release["releaseId"] for release in tagged])
            log(f"published {len(tagged)} tagged releases for artist {artist!r}")
            return len(tagged)
    raise SystemExit("[verify] no new tagged MusicBrainz releases found for top artists")


def store_count(spark, path, fmt="parquet"):
    if not Path(path).exists():
        return 0
    reader = spark.read.format(fmt) if fmt == "delta" else spark.read
    try:
        return reader.load(str(path)).count()
    except Exception:
        return 0


def wait_for(predicate, timeout, label):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(1)
    raise SystemExit(f"[verify] timed out waiting for {label}")


def main(argv=None):
    parser = argparse.ArgumentParser(description="Phase 2 end-to-end verification against Docker Kafka")
    parser.add_argument(
        "--wipe",
        action="store_true",
        help="delete checkpoints, stores and verification state first for a deterministic run",
    )
    args = parser.parse_args(argv)

    load_dotenv()
    cfg = load_config()
    api_key = os.environ.get(cfg["lastfm"]["api_key_env"])
    user_agent = os.environ.get(cfg["musicbrainz"]["user_agent_env"])
    if not api_key:
        raise SystemExit(f"missing {cfg['lastfm']['api_key_env']} in .env")
    if not user_agent:
        raise SystemExit(f"missing {cfg['musicbrainz']['user_agent_env']} in .env")

    wait_for_broker(cfg)
    log("broker reachable, topics verified")

    if args.wipe:
        wipe_paths(cfg)

    spark = build_spark(cfg)
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    producer = make_producer(cfg)
    raw_producer = KafkaProducer(
        bootstrap_servers=cfg["kafka"]["bootstrap_servers"],
        key_serializer=Utf8StringSerializer(),
    )
    exit_code = 0
    events_query = None
    trending_query = None
    try:
        seen = SeenStore(ROOT / "data" / "state" / "verify_catalog_seen.json")
        published, matched, _ = retry_call(
            lambda: poll_catalog(cfg, user_agent, seen, producer, limiter=RateLimiter(1.0)),
            label="catalog poll",
        )
        log(f"catalog poller cycle: {published} new of {matched} matched")

        events = collect_events(cfg, api_key)
        if len(events) < 100:
            raise SystemExit(f"[verify] only {len(events)} events fetched; need at least 100")

        tagged = publish_tagged_releases(cfg, user_agent, events, producer, seen)
        catalog_query = catalog_stream.start_query(
            spark, cfg, starting_offsets="earliest", available_now=True
        )
        catalog_query.awaitTermination()
        catalog_rows = store_count(spark, ROOT / cfg["paths"]["catalog_store"], fmt="delta")
        if catalog_rows <= 0:
            raise SystemExit("[verify] catalog store is empty after drain")
        log(f"catalog query drained: {catalog_rows} releases in Delta ({tagged} tagged for events)")

        malformed_payloads = [value.encode("utf-8") for value in MALFORMED]
        for index, payload in enumerate(malformed_payloads):
            raw_producer.send(
                cfg["kafka"]["topics"]["listening_events"],
                key=f"malformed-{index}",
                value=payload,
            )
        raw_producer.flush()
        log(f"published {len(malformed_payloads)} deliberately malformed messages")

        total = len(events)
        split = max(1, int(total * WAVE1_FRACTION))
        wave1, wave2 = events[:split], events[split:]
        for event in wave1:
            producer.send(
                cfg["kafka"]["topics"]["listening_events"],
                key=event["userId"],
                value=event,
            )
        producer.flush()
        log(f"published wave 1: {len(wave1)} of {total} events (backlog for query start)")

        events_query, malformed_counter = events_stream.start_query(
            spark, cfg, starting_offsets="earliest"
        )
        trending_query = trending_stream.start_query(spark, cfg, starting_offsets="earliest")
        log(f"two queries running: {events_query.id} + {trending_query.id}")
        log(f"events status: {events_query.status}")
        log(f"trending status: {trending_query.status}")

        events_path = ROOT / cfg["paths"]["events_store"]
        trending_path = ROOT / cfg["paths"]["trending_store"]
        wait_for(lambda: store_count(spark, str(events_path)) > 0, 600, "events store to receive wave 1")
        wait_for(lambda: store_count(spark, str(trending_path)) > 0, 600, "trending store to receive wave 1")
        killed_at = store_count(spark, str(events_path))
        trending_rows = store_count(spark, str(trending_path))
        if not 0 < killed_at < total:
            raise SystemExit(f"[verify] kill point {killed_at} not strictly inside (0, {total})")
        log(f"two queries confirmed active: events rows={killed_at}, trending rows={trending_rows}")

        events_query.stop()
        events_query = None
        log(f"events query killed mid-stream at {killed_at} of {total} events")

        for event in wave2:
            producer.send(
                cfg["kafka"]["topics"]["listening_events"],
                key=event["userId"],
                value=event,
            )
        producer.flush()
        log(f"published wave 2 while query was dead: {len(wave2)} events")

        restarted, _ = events_stream.start_query(
            spark, cfg, starting_offsets="earliest", available_now=True
        )
        restarted.awaitTermination()
        log("events query restarted from checkpoint and drained the backlog")

        time.sleep(3)
        final_rows = store_count(spark, str(events_path))
        distinct_rows = (
            spark.read.parquet(str(events_path))
            .select("userId", "trackName", "artistName", "eventTime")
            .distinct()
            .count()
        )
        genre_rows = (
            spark.read.parquet(str(events_path)).filter("genre IS NOT NULL").count()
        )
        malformed_seen = malformed_counter.value
        trending_rows = store_count(spark, str(trending_path))

        checks = [
            ("no loss: final rows == published events", final_rows == total),
            ("no duplicates: rows == distinct rows", final_rows == distinct_rows),
            ("kill was strictly mid-stream", 0 < killed_at < final_rows),
            ("accumulator counted every malformed message", malformed_seen == len(MALFORMED)),
            ("enrichment attached genre to events", genre_rows > 0),
            ("second query wrote trending windows", trending_rows > 0),
            ("catalog Delta table populated", catalog_rows > 0),
        ]
        for label, passed in checks:
            log(f"{'PASS' if passed else 'FAIL'}: {label}")
        log(
            f"totals: published={total}, killed_at={killed_at}, final={final_rows}, "
            f"distinct={distinct_rows}, malformed={malformed_seen}/{len(MALFORMED)}, "
            f"genre_rows={genre_rows}, trending_rows={trending_rows}, catalog_rows={catalog_rows}"
        )
        if not all(passed for _, passed in checks):
            exit_code = 1
        else:
            log("PHASE 2 VERIFICATION: ALL CHECKS PASSED")
    finally:
        if events_query is not None:
            events_query.stop()
        if trending_query is not None:
            trending_query.stop()
        producer.close()
        raw_producer.close()
        spark.stop()
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
