import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import json

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import BinaryType, StructField, StructType

from src.common.config import load_config
from src.ingestion.catalog_stream import (
    QUERY_NAME,
    parse_catalog,
    prepare_catalog,
    upsert_catalog,
)
from src.ingestion.events_stream import CATALOG_ENRICH_SCHEMA, read_catalog_snapshot

RAW_SCHEMA = StructType([StructField("value", BinaryType())])

VALID_RELEASE = {
    "releaseId": "mbid-123",
    "title": "New Album",
    "artist": "New Artist",
    "releaseDate": "2026-11-21",
    "genre": "synthpop",
    "sourceTag": "live",
}


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    existing = SparkSession.getActiveSession()
    if existing is not None:
        existing.stop()
    cfg = load_config()
    builder = SparkSession.builder.appName("CatalogStreamTests").master("local[1]")
    for key, value in cfg["spark"]["config"].items():
        builder = builder.config(key, value)
    builder = builder.config("spark.sql.shuffle.partitions", "4")
    session = builder.getOrCreate()
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


def release(release_id, title, genre="synthpop"):
    return dict(VALID_RELEASE, releaseId=release_id, title=title, genre=genre)


def test_parse_catalog_maps_valid_payload(spark):
    row = prepare_catalog(raw_df(spark, [VALID_RELEASE])).collect()[0]
    assert row["releaseId"] == "mbid-123"
    assert row["title"] == "New Album"
    assert row["artist"] == "New Artist"
    assert row["releaseDate"] == "2026-11-21"
    assert row["genre"] == "synthpop"
    assert row["sourceTag"] == "live"


def test_prepare_catalog_drops_malformed(spark):
    rows = [VALID_RELEASE, "this is not json", {"title": "no id"}]
    assert prepare_catalog(raw_df(spark, rows)).count() == 1
    parsed = parse_catalog(raw_df(spark, ["this is not json"])).collect()[0]
    assert parsed["releaseId"] is None
    assert parsed["isValid"] is False


def test_upsert_catalog_inserts_updates_and_absorbs_duplicates(spark, tmp_path):
    table_path = str(tmp_path / "catalog")
    first = prepare_catalog(raw_df(spark, [release("r1", "Title One"), release("r2", "Title Two")]))
    upsert_catalog(first, 0, table_path)

    second = prepare_catalog(
        raw_df(
            spark,
            [
                release("r1", "Title One Updated", genre="new-wave"),
                release("r3", "Title Three"),
            ],
        )
    )
    upsert_catalog(second, 1, table_path)
    upsert_catalog(second, 2, table_path)

    rows = spark.read.format("delta").load(table_path).orderBy("releaseId").collect()
    assert [r["releaseId"] for r in rows] == ["r1", "r2", "r3"]
    assert rows[0]["title"] == "Title One Updated"
    assert rows[0]["genre"] == "new-wave"
    assert rows[1]["title"] == "Title Two"
    assert rows[1]["genre"] == "synthpop"
    assert rows[2]["title"] == "Title Three"


def test_upsert_catalog_absorbs_within_batch_duplicates(spark, tmp_path):
    table_path = str(tmp_path / "catalog")
    batch = prepare_catalog(
        raw_df(
            spark,
            [
                release("r1", "Title One"),
                release("r1", "Title One"),
                release("r2", "Title Two"),
            ],
        )
    )
    upsert_catalog(batch, 0, table_path)
    rows = spark.read.format("delta").load(table_path).orderBy("releaseId").collect()
    assert [r["releaseId"] for r in rows] == ["r1", "r2"]


def test_catalog_stream_merges_streaming_output(spark, tmp_path):
    src = tmp_path / "src"
    table_path = tmp_path / "catalog"
    checkpoint = tmp_path / "checkpoint"
    src.mkdir()
    lines = [json.dumps(release("r1", "Title One")), json.dumps(release("r2", "Title Two"))]
    (src / "part1.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")

    raw = spark.readStream.format("text").load(str(src))
    query = (
        prepare_catalog(raw)
        .writeStream.queryName(QUERY_NAME)
        .option("checkpointLocation", str(checkpoint))
        .foreachBatch(lambda batch_df, batch_id: upsert_catalog(batch_df, batch_id, str(table_path)))
        .trigger(availableNow=True)
        .start()
    )
    try:
        query.awaitTermination()
        rows = spark.read.format("delta").load(str(table_path)).orderBy("releaseId").collect()
        assert [r["releaseId"] for r in rows] == ["r1", "r2"]
        assert rows[0]["title"] == "Title One"
    finally:
        query.stop()


def test_read_catalog_snapshot_returns_delta_rows_or_empty(spark, tmp_path):
    table_path = str(tmp_path / "catalog")
    upsert_catalog(prepare_catalog(raw_df(spark, [VALID_RELEASE])), 0, table_path)

    snapshot = read_catalog_snapshot(spark, table_path)
    assert snapshot.count() == 1
    assert "genre" in snapshot.columns

    missing = read_catalog_snapshot(spark, str(tmp_path / "missing"))
    assert missing.count() == 0
    assert missing.columns == list(CATALOG_ENRICH_SCHEMA.names)
