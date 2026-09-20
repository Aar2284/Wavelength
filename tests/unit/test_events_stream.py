import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import json
from datetime import date, datetime, timezone

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import BinaryType, StructField, StructType

from src.ingestion.events_stream import (
    DEDUP_KEYS,
    OUTPUT_COLUMNS,
    apply_stateful_ops,
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
    write_valid_partitioned(df, 0, str(tmp_path))

    written = spark.read.parquet(str(tmp_path))
    assert written.columns == OUTPUT_COLUMNS
    assert written.count() == 1
    assert written.collect()[0]["userId"] == "alice"
    assert (tmp_path / "date=2023-11-14").exists()
