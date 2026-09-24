import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import json
import time
from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import BinaryType, StructField, StructType

from src.ingestion.events_stream import (
    CATALOG_ENRICH_SCHEMA,
    DEDUP_KEYS,
    OUTPUT_COLUMNS,
    QUERY_NAME,
    MalformedEventCounter,
    apply_stateful_ops,
    attach_malformed_counter,
    enrich_events,
    parse_events,
    prepare_events,
    write_valid_partitioned,
)

RAW_SCHEMA = StructType([StructField("value", BinaryType())])

VALID_PAYLOAD = {
    "userId": "alice",
    "trackName": "New Song",
    "artistName": "New Artist",
    "timestamp": "2023-11-14T22:13:20+00:00",
    "nowPlaying": False,
    "source": "live",
}

EXPECTED_EVENT_TIME = (
    datetime(2023, 11, 14, 22, 13, 20, tzinfo=timezone.utc).astimezone().replace(tzinfo=None)
)


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.appName("EventsStreamTests")
        .master("local[1]")
        .getOrCreate()
    )
    session.conf.set("spark.sql.session.timeZone", "UTC")
    session.conf.set("spark.sql.timestampFormat", "yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]")
    session.conf.set("spark.sql.shuffle.partitions", "4")
    yield session
    session.stop()


def raw_df(spark: SparkSession, values):
    rows = []
    for value in values:
        if isinstance(value, dict):
            value = json.dumps(value).encode("utf-8")
        elif isinstance(value, str):
            value = value.encode("utf-8")
        rows.append((value,))
    return spark.createDataFrame(rows, schema=RAW_SCHEMA)


def catalog_df(spark: SparkSession, rows):
    return spark.createDataFrame(rows, schema=CATALOG_ENRICH_SCHEMA)


def test_parse_events_maps_valid_payload(spark):
    row = prepare_events(raw_df(spark, [VALID_PAYLOAD])).collect()[0]
    assert row["userId"] == "alice"
    assert row["trackName"] == "New Song"
    assert row["artistName"] == "New Artist"
    assert row["eventTime"] == EXPECTED_EVENT_TIME
    assert row["date"] == date(2023, 11, 14)
    assert row["nowPlaying"] is False
    assert row["source"] == "live"
    assert row["isValid"] is True


def test_parse_events_tolerates_missing_optional_fields(spark):
    payload = {
        "userId": "bob",
        "trackName": "Old Song",
        "artistName": "Old Artist",
        "timestamp": "2023-11-14T22:13:20+00:00",
    }
    row = prepare_events(raw_df(spark, [payload])).collect()[0]
    assert row["source"] is None
    assert row["nowPlaying"] is None
    assert row["isValid"] is True


def test_parse_events_flags_malformed_json(spark):
    parsed = parse_events(raw_df(spark, ["this is not json"])).collect()[0]
    assert parsed["userId"] is None
    assert parsed["eventTime"] is None
    assert parsed["isValid"] is False
    assert prepare_events(raw_df(spark, ["this is not json"])).count() == 0


def test_parse_events_flags_missing_required_field(spark):
    payload = {
        "trackName": "No User Song",
        "artistName": "Someone",
        "timestamp": "2023-11-14T22:13:20+00:00",
    }
    row = prepare_events(raw_df(spark, [payload])).collect()[0]
    assert row["userId"] is None
    assert row["eventTime"] == EXPECTED_EVENT_TIME
    assert row["isValid"] is False


def test_prepare_events_drops_unparseable_timestamp(spark):
    payload = {
        "userId": "carol",
        "trackName": "Bad Time",
        "artistName": "Someone",
        "timestamp": "not-a-timestamp",
    }
    assert prepare_events(raw_df(spark, [payload])).count() == 0


def test_dedup_collapses_identical_events(spark):
    other = dict(VALID_PAYLOAD, timestamp="2023-11-14T23:00:00+00:00")
    df = raw_df(spark, [VALID_PAYLOAD, VALID_PAYLOAD, other])
    deduped = apply_stateful_ops(prepare_events(df))
    assert deduped.count() == 2
    assert deduped.select(*DEDUP_KEYS).distinct().count() == 2


def test_write_valid_partitioned_writes_only_valid_rows(spark, tmp_path):
    bad = {
        "trackName": "No User",
        "artistName": "Someone",
        "timestamp": "2023-11-14T22:13:20+00:00",
    }
    df = parse_events(raw_df(spark, [VALID_PAYLOAD, bad]))
    enriched = enrich_events(df, catalog_df(spark, []))
    write_valid_partitioned(enriched, 0, str(tmp_path))

    written = spark.read.parquet(str(tmp_path))
    assert written.columns == OUTPUT_COLUMNS
    assert written.count() == 1
    assert written.collect()[0]["userId"] == "alice"
    assert (tmp_path / "date=2023-11-14").exists()


def progress_event(name, metrics):
    return SimpleNamespace(
        progress=SimpleNamespace(name=name, observedMetrics=metrics)
    )


def test_counter_accumulates_malformed_from_progress(spark):
    accumulator = spark.sparkContext.accumulator(0)
    counter = MalformedEventCounter(accumulator, QUERY_NAME)

    counter.onQueryProgress(
        progress_event(QUERY_NAME, {"validation": Row(total=10, invalid=3)})
    )
    counter.onQueryProgress(
        progress_event(QUERY_NAME, {"validation": Row(total=5, invalid=2)})
    )

    assert counter.value == 5


def test_counter_ignores_other_queries_and_empty_metrics(spark):
    accumulator = spark.sparkContext.accumulator(0)
    counter = MalformedEventCounter(accumulator, QUERY_NAME)

    counter.onQueryProgress(progress_event("some_other_query", {"validation": Row(total=9, invalid=9)}))
    counter.onQueryProgress(progress_event(QUERY_NAME, {}))
    counter.onQueryProgress(progress_event(QUERY_NAME, {"validation": Row(total=5, invalid=0)}))
    counter.onQueryProgress(progress_event(QUERY_NAME, {"validation": Row(total=5, invalid=None)}))

    assert counter.value == 0


def test_enrich_events_attaches_genre_and_picks_latest_release(spark):
    events = prepare_events(raw_df(spark, [VALID_PAYLOAD]))
    catalog = catalog_df(
        spark,
        [
            ("New Artist", "indie", "2026-11-21", "rel-new"),
            ("new artist", "synthpop", "2026-01-01", "rel-old"),
        ],
    )
    row = enrich_events(events, catalog).collect()[0]
    assert row["genre"] == "indie"
    assert len(row["songId"]) == 16
    assert all(c in "0123456789abcdef" for c in row["songId"])


def test_enrich_events_null_genre_for_unknown_artist_and_stable_song_id(spark):
    catalog = catalog_df(spark, [("New Artist", "electronic", "2026-11-21", "rel-1")])
    variants = [
        VALID_PAYLOAD,
        dict(VALID_PAYLOAD, userId="bob", artistName="  NEW ARTIST ", trackName=" new song "),
        dict(VALID_PAYLOAD, userId="carol", trackName="Different Song"),
        dict(VALID_PAYLOAD, userId="dave", artistName="Unknown Band"),
    ]
    rows = enrich_events(prepare_events(raw_df(spark, variants)), catalog).collect()
    by_user = {r["userId"]: r for r in rows}
    assert by_user["alice"]["genre"] == "electronic"
    assert by_user["bob"]["genre"] == "electronic"
    assert by_user["alice"]["songId"] == by_user["bob"]["songId"]
    assert by_user["carol"]["songId"] != by_user["alice"]["songId"]
    assert by_user["dave"]["genre"] is None
    assert len(by_user["dave"]["songId"]) == 16


def test_stream_pipeline_counts_malformed_and_writes_valid(spark, tmp_path):
    src = tmp_path / "src"
    out = tmp_path / "out"
    checkpoint = tmp_path / "checkpoint"
    src.mkdir()
    other = dict(VALID_PAYLOAD, userId="bob", timestamp="2023-11-14T23:00:00+00:00")
    missing_user = {
        "trackName": "No User",
        "artistName": "Someone",
        "timestamp": "2023-11-14T22:13:20+00:00",
    }
    lines = [
        json.dumps(VALID_PAYLOAD),
        "this is not json",
        json.dumps(missing_user),
        json.dumps(other),
    ]
    (src / "part1.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")

    counter = attach_malformed_counter(spark, QUERY_NAME)
    raw = spark.readStream.format("text").load(str(src))
    catalog = catalog_df(spark, [("New Artist", "electronic", "2026-11-21", "rel-1")])
    query = (
        enrich_events(apply_stateful_ops(prepare_events(raw)), catalog)
        .writeStream.queryName(QUERY_NAME)
        .option("checkpointLocation", str(checkpoint))
        .foreachBatch(lambda batch_df, batch_id: write_valid_partitioned(batch_df, batch_id, str(out)))
        .trigger(availableNow=True)
        .start()
    )
    try:
        query.awaitTermination()
        deadline = time.monotonic() + 20
        while counter.value < 2 and time.monotonic() < deadline:
            time.sleep(0.2)
        assert counter.value == 2, f"expected 2 malformed, got {counter.value}"

        written = spark.read.parquet(str(out))
        assert written.count() == 2
        rows = written.collect()
        assert sorted(r["userId"] for r in rows) == ["alice", "bob"]
        assert all(r["genre"] == "electronic" for r in rows)
        assert all(r["songId"] == rows[0]["songId"] for r in rows)
    finally:
        query.stop()
        spark.streams.removeListener(counter)
