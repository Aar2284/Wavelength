import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import pytest
from pyspark.sql import SparkSession

from src.common.config import load_config

KAFKA_BATCH_COLUMNS = {
    "key",
    "value",
    "topic",
    "partition",
    "offset",
    "timestamp",
    "timestampType",
}


def _ensure_hadoop_home() -> None:
    if os.environ.get("HADOOP_HOME"):
        return
    for candidate in (r"C:\hadoop",):
        if os.path.isfile(os.path.join(candidate, "bin", "winutils.exe")):
            os.environ["HADOOP_HOME"] = candidate
            return


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    _ensure_hadoop_home()
    if not os.environ.get("HADOOP_HOME"):
        pytest.skip(
            "HADOOP_HOME unset and no winutils.exe found — "
            "install winutils and set HADOOP_HOME to its parent dir"
        )

    existing = SparkSession.getActiveSession()
    if existing is not None:
        existing.stop()

    cfg = load_config()
    builder = SparkSession.builder.appName("SparkKafkaIntegration").master("local[1]")
    for key, value in cfg["spark"]["config"].items():
        builder = builder.config(key, value)

    session = builder.getOrCreate()
    yield session
    session.stop()


def _kafka_read(spark: SparkSession, topic: str, streaming: bool):
    cfg = load_config()
    reader = spark.readStream if streaming else spark.read
    return (
        reader.format("kafka")
        .option("kafka.bootstrap.servers", cfg["kafka"]["bootstrap_servers"])
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )


def test_spark_reads_kafka_batch(spark: SparkSession):
    cfg = load_config()
    topic = cfg["kafka"]["topics"]["listening_events"]

    df = _kafka_read(spark, topic, streaming=False)

    missing = KAFKA_BATCH_COLUMNS - set(df.columns)
    assert not missing, f"missing kafka columns: {missing}"

    count = df.count()
    assert count > 0, f"expected at least one message in {topic}"

    sample = df.select("topic", "partition", "value").limit(1).collect()[0]
    assert sample["topic"] == topic
    assert sample["value"] is not None

    print(f"[PASS] batch read: {count} messages from {topic}")


def test_spark_reads_kafka_stream(spark: SparkSession, tmp_path):
    cfg = load_config()
    topic = cfg["kafka"]["topics"]["catalog_releases"]

    df = _kafka_read(spark, topic, streaming=True)
    query = (
        df.writeStream.format("memory")
        .queryName("kafka_stream_probe")
        .option("checkpointLocation", str(tmp_path / "checkpoint"))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination(timeout=120)

    count = spark.sql("SELECT COUNT(*) AS c FROM kafka_stream_probe").collect()[0]["c"]
    assert count > 0, f"streaming query returned no rows from {topic}"

    print(f"[PASS] streaming read: {count} messages from {topic}")
