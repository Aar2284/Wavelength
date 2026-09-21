import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import json
from datetime import datetime, timezone

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import BinaryType, StringType, StructField, StructType, TimestampType

from src.ingestion.trending_stream import (
    QUERY_NAME,
    prepare_trending,
    windowed_plays,
    write_trending,
)

RAW_SCHEMA = StructType([StructField("value", BinaryType())])

ALICE_EARLY = {
    "userId": "alice",
    "trackName": "New Song",
    "artistName": "Artist A",
    "timestamp": "2026-11-21T10:00:15+00:00",
    "nowPlaying": False,
    "source": "live",
}

ALICE_LATER = dict(ALICE_EARLY, timestamp="2026-11-21T10:00:45+00:00")

BOB_TRAILING = {
    "userId": "bob",
    "trackName": "Other Song",
    "artistName": "Artist B",
    "timestamp": "2026-11-21T10:10:00+00:00",
    "nowPlaying": False,
    "source": "live",
}

EXPECTED_WINDOW_START = (
    datetime(2026, 11, 21, 10, 0, 0, tzinfo=timezone.utc).astimezone().replace(tzinfo=None)
)


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.appName("TrendingStreamTests")
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


def test_windowed_plays_counts_per_minute(spark):
    schema = StructType(
        [
            StructField("eventTime", TimestampType()),
            StructField("artistName", StringType()),
        ]
    )
    rows = [
        (datetime(2026, 11, 21, 10, 0, 15), "Artist A"),
        (datetime(2026, 11, 21, 10, 0, 45), "Artist A"),
        (datetime(2026, 11, 21, 10, 3, 10), "Artist B"),
    ]
    result = windowed_plays(spark.createDataFrame(rows, schema))
    assert result.columns == ["windowStart", "windowEnd", "artistName", "plays"]
    assert {
        (r["windowStart"], r["artistName"]): r["plays"] for r in result.collect()
    } == {
        (datetime(2026, 11, 21, 10, 0, 0), "Artist A"): 2,
        (datetime(2026, 11, 21, 10, 3, 0), "Artist B"): 1,
    }


def test_prepare_trending_filters_invalid_and_dedupes(spark):
    missing_user = {
        "trackName": "No User",
        "artistName": "Artist A",
        "timestamp": "2026-11-21T10:00:15+00:00",
    }
    rows = [ALICE_EARLY, "this is not json", missing_user, ALICE_EARLY, BOB_TRAILING]
    out = prepare_trending(raw_df(spark, rows)).collect()
    assert len(out) == 2
    assert sorted(r["userId"] for r in out) == ["alice", "bob"]


def test_trending_stream_emits_closed_windows(spark, tmp_path):
    src = tmp_path / "src"
    out = tmp_path / "out"
    checkpoint = tmp_path / "checkpoint"
    src.mkdir()
    lines = [
        json.dumps(ALICE_EARLY),
        json.dumps(ALICE_LATER),
        json.dumps(BOB_TRAILING),
    ]
    (src / "part1.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")

    raw = spark.readStream.format("text").load(str(src))
    query = (
        windowed_plays(prepare_trending(raw))
        .writeStream.queryName(QUERY_NAME)
        .outputMode("append")
        .option("checkpointLocation", str(checkpoint))
        .foreachBatch(lambda batch_df, batch_id: write_trending(batch_df, batch_id, str(out)))
        .trigger(availableNow=True)
        .start()
    )
    try:
        query.awaitTermination()
        written = spark.read.parquet(str(out))
        assert written.columns == ["windowStart", "windowEnd", "artistName", "plays"]
        rows = written.collect()
        assert [(r["windowStart"], r["artistName"], r["plays"]) for r in rows] == [
            (EXPECTED_WINDOW_START, "Artist A", 2)
        ]
    finally:
        query.stop()
