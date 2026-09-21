import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import load_config
from src.ingestion.events_stream import apply_stateful_ops, build_spark, parse_events

ROOT = Path(__file__).resolve().parents[2]

QUERY_NAME = "trending_stream"
WINDOW_DURATION = "1 minute"
WATERMARK_DELAY = "2 minutes"
GROUP_KEY = "artistName"


def prepare_trending(raw_df: DataFrame) -> DataFrame:
    return apply_stateful_ops(parse_events(raw_df).filter(F.col("isValid")), WATERMARK_DELAY)


def windowed_plays(events_df: DataFrame, key_col: str = GROUP_KEY) -> DataFrame:
    return (
        events_df.groupBy(F.window("eventTime", WINDOW_DURATION), F.col(key_col))
        .agg(F.count(F.lit(1)).alias("plays"))
        .select(
            F.col("window.start").alias("windowStart"),
            F.col("window.end").alias("windowEnd"),
            F.col(key_col),
            F.col("plays"),
        )
    )


def write_trending(batch_df: DataFrame, batch_id: int, output_path: str) -> None:
    batch_df.write.mode("append").parquet(output_path)


def start_query(
    spark: SparkSession,
    cfg: dict,
    starting_offsets: str = "latest",
    output_path: str = None,
    checkpoint_path: str = None,
    available_now: bool = False,
):
    output_path = output_path or str(ROOT / cfg["paths"]["trending_store"])
    checkpoint_path = checkpoint_path or str(ROOT / cfg["paths"]["checkpoints"] / "trending")
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg["kafka"]["bootstrap_servers"])
        .option("subscribe", cfg["kafka"]["topics"]["listening_events"])
        .option("startingOffsets", starting_offsets)
        .load()
    )
    writer = (
        windowed_plays(prepare_trending(raw))
        .writeStream.queryName(QUERY_NAME)
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .foreachBatch(lambda batch_df, batch_id: write_trending(batch_df, batch_id, output_path))
    )
    if available_now:
        writer = writer.trigger(availableNow=True)
    return writer.start()


def main(argv=None):
    parser = argparse.ArgumentParser(description="plays-per-minute trending Structured Streaming query")
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
    query = start_query(spark, cfg, starting_offsets=args.starting_offsets, available_now=args.once)
    print(f"[trending-stream] query started: {query.id} ({'once' if args.once else 'continuous'})")
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        query.stop()
    finally:
        print("[trending-stream] query finished")
        spark.stop()


if __name__ == "__main__":
    main()
