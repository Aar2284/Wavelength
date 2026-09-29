import argparse
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from src.common.config import load_config
from src.ingestion.events_stream import build_spark

ROOT = Path(__file__).resolve().parents[2]

QUERY_NAME = "catalog_stream"
CATALOG_SCHEMA = StructType(
    [
        StructField("releaseId", StringType()),
        StructField("title", StringType()),
        StructField("artist", StringType()),
        StructField("releaseDate", StringType()),
        StructField("genre", StringType()),
        StructField("sourceTag", StringType()),
    ]
)
OUTPUT_COLUMNS = ["releaseId", "title", "artist", "releaseDate", "genre", "sourceTag"]
CHANGE_CONDITION = (
    "NOT (target.title <=> source.title AND target.artist <=> source.artist "
    "AND target.releaseDate <=> source.releaseDate AND target.genre <=> source.genre "
    "AND target.sourceTag <=> source.sourceTag)"
)


def parse_catalog(raw_df: DataFrame) -> DataFrame:
    return raw_df.withColumn("payload", F.from_json(F.col("value").cast("string"), CATALOG_SCHEMA)).select(
        F.col("payload.releaseId").alias("releaseId"),
        F.col("payload.title").alias("title"),
        F.col("payload.artist").alias("artist"),
        F.col("payload.releaseDate").alias("releaseDate"),
        F.col("payload.genre").alias("genre"),
        F.col("payload.sourceTag").alias("sourceTag"),
        F.col("payload.releaseId").isNotNull().alias("isValid"),
    )


def prepare_catalog(raw_df: DataFrame) -> DataFrame:
    return parse_catalog(raw_df).filter(F.col("isValid"))


def upsert_catalog(batch_df: DataFrame, batch_id: int, table_path: str) -> None:
    spark = batch_df.sparkSession
    source = batch_df.select(*OUTPUT_COLUMNS).dropDuplicates(["releaseId"])
    if not DeltaTable.isDeltaTable(spark, table_path):
        source.write.format("delta").mode("overwrite").save(table_path)
    (
        DeltaTable.forPath(spark, table_path)
        .alias("target")
        .merge(source.alias("source"), "target.releaseId = source.releaseId")
        .whenMatchedUpdateAll(condition=CHANGE_CONDITION)
        .whenNotMatchedInsertAll()
        .execute()
    )


def start_query(
    spark: SparkSession,
    cfg: dict,
    starting_offsets: str = "latest",
    table_path: str = None,
    checkpoint_path: str = None,
    available_now: bool = False,
):
    table_path = table_path or str(ROOT / cfg["paths"]["catalog_store"])
    checkpoint_path = checkpoint_path or str(ROOT / cfg["paths"]["checkpoints"] / "catalog")
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg["kafka"]["bootstrap_servers"])
        .option("subscribe", cfg["kafka"]["topics"]["catalog_releases"])
        .option("startingOffsets", starting_offsets)
        .load()
    )
    writer = (
        prepare_catalog(raw)
        .writeStream.queryName(QUERY_NAME)
        .option("checkpointLocation", checkpoint_path)
        .foreachBatch(lambda batch_df, batch_id: upsert_catalog(batch_df, batch_id, table_path))
    )
    if available_now:
        writer = writer.trigger(availableNow=True)
    return writer.start()


def main(argv=None):
    parser = argparse.ArgumentParser(description="catalog-releases Delta upsert Structured Streaming query")
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
    print(f"[catalog-stream] query started: {query.id} ({'once' if args.once else 'continuous'})")
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        query.stop()
    finally:
        print("[catalog-stream] query finished")
        spark.stop()


if __name__ == "__main__":
    main()
