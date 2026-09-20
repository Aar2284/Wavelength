import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, StructField, StructType

from src.common.config import load_config

ROOT = Path(__file__).resolve().parents[2]

EVENT_SCHEMA = StructType(
    [
        StructField("userId", StringType()),
        StructField("trackName", StringType()),
        StructField("artistName", StringType()),
        StructField("timestamp", StringType()),
        StructField("nowPlaying", BooleanType()),
        StructField("source", StringType()),
    ]
)

DEDUP_KEYS = ["userId", "artistName", "trackName", "eventTime"]
WATERMARK_DELAY = "30 minutes"
OUTPUT_COLUMNS = [
    "userId",
    "trackName",
    "artistName",
    "eventTime",
    "nowPlaying",
    "source",
    "date",
]


def parse_events(raw_df: DataFrame) -> DataFrame:
    return (
        raw_df.withColumn("payload", F.from_json(F.col("value").cast("string"), EVENT_SCHEMA))
        .withColumn("userId", F.col("payload.userId"))
        .withColumn("trackName", F.col("payload.trackName"))
        .withColumn("artistName", F.col("payload.artistName"))
        .withColumn("nowPlaying", F.col("payload.nowPlaying"))
        .withColumn("source", F.col("payload.source"))
        .withColumn("eventTime", F.try_to_timestamp(F.col("payload.timestamp")))
        .withColumn("date", F.to_date(F.col("eventTime")))
        .withColumn(
            "isValid",
            F.col("userId").isNotNull()
            & F.col("trackName").isNotNull()
            & F.col("artistName").isNotNull()
            & F.col("eventTime").isNotNull(),
        )
        .drop("payload")
    )


def prepare_events(raw_df: DataFrame) -> DataFrame:
    return parse_events(raw_df).filter(F.col("eventTime").isNotNull())


def apply_stateful_ops(events_df: DataFrame) -> DataFrame:
    return events_df.withWatermark("eventTime", WATERMARK_DELAY).dropDuplicates(DEDUP_KEYS)


def write_valid_partitioned(batch_df: DataFrame, batch_id: int, output_path: str) -> None:
    (
        batch_df.filter(F.col("isValid"))
        .select(*OUTPUT_COLUMNS)
        .write.mode("append")
        .partitionBy("date")
        .parquet(output_path)
    )


def start_query(
    spark: SparkSession,
    cfg: dict,
    starting_offsets: str = "latest",
    output_path: str = None,
    checkpoint_path: str = None,
    available_now: bool = False,
):
    output_path = output_path or str(ROOT / cfg["paths"]["events_store"])
    checkpoint_path = checkpoint_path or str(ROOT / cfg["paths"]["checkpoints"] / "events")
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg["kafka"]["bootstrap_servers"])
        .option("subscribe", cfg["kafka"]["topics"]["listening_events"])
        .option("startingOffsets", starting_offsets)
        .load()
    )
    writer = (
        apply_stateful_ops(prepare_events(raw))
        .writeStream.queryName("events_stream")
        .option("checkpointLocation", checkpoint_path)
        .foreachBatch(lambda batch_df, batch_id: write_valid_partitioned(batch_df, batch_id, output_path))
    )
    if available_now:
        writer = writer.trigger(availableNow=True)
    return writer.start()


def build_spark(cfg: dict) -> SparkSession:
    builder = SparkSession.builder.appName(cfg["spark"]["app_name"]).master(cfg["spark"]["master"])
    for key, value in cfg["spark"]["config"].items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def main(argv=None):
    parser = argparse.ArgumentParser(description="listening-events Structured Streaming query")
    parser.add_argument(
        "--once",
        action="store_true",
        help="process the current backlog and exit instead of running continuously",
    )
    parser.add_argument(
        "--starting-offsets",
        choices=["earliest", "latest"],
        default="latest",
        help="where a fresh checkpoint begins reading the topic",
    )
    args = parser.parse_args(argv)

    cfg = load_config()
    spark = build_spark(cfg)
    query = start_query(
        spark, cfg, starting_offsets=args.starting_offsets, available_now=args.once
    )
    print(f"[events-stream] query started: {query.id} ({'once' if args.once else 'continuous'})")
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        query.stop()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
