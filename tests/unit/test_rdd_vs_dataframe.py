import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import pytest
from pyspark.sql import SparkSession

from src.benchmarks.rdd_vs_dataframe import (
    GENRES,
    aggregate_dataframe,
    aggregate_rdd,
    make_fixture,
    write_chart,
)

EXPECTED = {
    "rock": (150.0, 0.5),
    "jazz": (80.0, 1 / 3),
}


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.appName("RddVsDataFrameTests")
        .master("local[1]")
        .getOrCreate()
    )
    session.conf.set("spark.sql.shuffle.partitions", "4")
    yield session
    session.stop()


def test_aggregations_match_known_values(spark):
    rows = [
        ("rock", 100, False),
        ("rock", 200, True),
        ("jazz", 60, False),
        ("jazz", 60, False),
        ("jazz", 120, True),
    ]
    df = spark.createDataFrame(
        rows, "genre string, playDurationSec int, skipped boolean"
    )
    for result in (aggregate_dataframe(df), aggregate_rdd(df)):
        assert set(result) == set(EXPECTED)
        for genre, (duration, skip_rate) in EXPECTED.items():
            assert result[genre][0] == pytest.approx(duration)
            assert result[genre][1] == pytest.approx(skip_rate)


def test_fixture_is_deterministic(spark):
    first = make_fixture(spark, 500).collect()
    second = make_fixture(spark, 500).collect()
    assert first == second
    assert {row.genre for row in first} <= set(GENRES)
    assert all(30 <= row.playDurationSec < 270 for row in first)


def test_write_chart_creates_png(tmp_path):
    records = [
        {"rows": 1_000, "dataframe_seconds": 0.5, "rdd_seconds": 0.8},
        {"rows": 10_000, "dataframe_seconds": 1.2, "rdd_seconds": 2.5},
    ]
    path = tmp_path / "chart.png"
    write_chart(records, path)
    assert path.exists()
    assert path.stat().st_size > 0
