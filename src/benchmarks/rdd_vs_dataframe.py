import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import load_config

GENRES = (
    "rock",
    "electronic",
    "hip-hop",
    "jazz",
    "pop",
    "classical",
    "metal",
    "folk",
)
SIZES = (10_000, 100_000, 1_000_000)
REPEATS = 3
CHART_NAME = "rdd_vs_dataframe.png"
JSON_NAME = "rdd_vs_dataframe.json"


def make_fixture(spark: SparkSession, n_rows: int) -> DataFrame:
    genres = F.array([F.lit(genre) for genre in GENRES])
    genre_index = (F.col("id") % F.lit(len(GENRES))).cast("int")
    return spark.range(n_rows).select(
        F.element_at(genres, genre_index + F.lit(1)).alias("genre"),
        (
            ((F.col("id") * F.lit(37)) % F.lit(240)) + F.lit(30)
        ).cast("int").alias("playDurationSec"),
        (F.col("id") % F.lit(5) == F.lit(0)).alias("skipped"),
    )


def _sequence_sum(acc, value):
    return (acc[0] + value[0], acc[1] + 1, acc[2] + value[2])


def _combination_sum(left, right):
    return (
        left[0] + right[0],
        left[1] + right[1],
        left[2] + right[2],
    )


def aggregate_rdd(df: DataFrame) -> dict:
    keyed = df.rdd.map(
        lambda row: (
            row.genre,
            (float(row.playDurationSec), 1, 1 if row.skipped else 0),
        )
    )
    summed = keyed.aggregateByKey(
        (0.0, 0, 0),
        _sequence_sum,
        _combination_sum,
    )
    return {
        genre: (total / count, skipped / count)
        for genre, (total, count, skipped) in sorted(summed.collectAsMap().items())
    }


def aggregate_dataframe(df: DataFrame) -> dict:
    grouped = df.groupBy("genre").agg(
        F.avg("playDurationSec").alias("avg_play_duration"),
        F.avg(F.col("skipped").cast("double")).alias("skip_rate"),
    )
    return {
        row.genre: (row.avg_play_duration, row.skip_rate)
        for row in grouped.orderBy("genre").collect()
    }


def median_seconds(fn, repeats: int = REPEATS) -> float:
    fn()
    times = []
    for _ in range(repeats):
        start = time.perf_counter()
        fn()
        times.append(time.perf_counter() - start)
    times.sort()
    return times[len(times) // 2]


def _human_rows(n_rows: int) -> str:
    if n_rows >= 1_000_000:
        return f"{n_rows // 1_000_000}M"
    if n_rows >= 1_000:
        return f"{n_rows // 1_000}k"
    return str(n_rows)


def write_chart(records: list, path: Path) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    labels = [_human_rows(record["rows"]) for record in records]
    positions = list(range(len(records)))
    width = 0.35
    dataframe_values = [record["dataframe_seconds"] for record in records]
    rdd_values = [record["rdd_seconds"] for record in records]

    figure, axis = plt.subplots(figsize=(9, 5))
    dataframe_bars = axis.bar(
        [p - width / 2 for p in positions],
        dataframe_values,
        width,
        label="DataFrame",
    )
    rdd_bars = axis.bar(
        [p + width / 2 for p in positions],
        rdd_values,
        width,
        label="RDD",
    )
    axis.set_xticks(positions)
    axis.set_xticklabels(labels)
    axis.set_xlabel("rows")
    axis.set_ylabel("median wall-clock seconds")
    axis.set_title("RDD vs DataFrame — avg play duration + skip rate per genre")
    axis.legend()
    for bars in (dataframe_bars, rdd_bars):
        for bar in bars:
            height = bar.get_height()
            axis.annotate(
                f"{height:.2f}s",
                xy=(bar.get_x() + bar.get_width() / 2, height),
                xytext=(0, 3),
                textcoords="offset points",
                ha="center",
                fontsize=8,
            )
    figure.tight_layout()
    figure.savefig(path, dpi=150)
    plt.close(figure)


def run_benchmark(
    spark: SparkSession,
    sizes: tuple = SIZES,
    repeats: int = REPEATS,
    output_dir=None,
) -> dict:
    cfg = load_config()
    destination = Path(output_dir) if output_dir else Path(cfg["paths"]["results"])
    destination.mkdir(parents=True, exist_ok=True)

    records = []
    for size in sizes:
        fixture = make_fixture(spark, size).persist()
        fixture.count()
        dataframe_seconds = median_seconds(
            lambda: aggregate_dataframe(fixture), repeats
        )
        rdd_seconds = median_seconds(lambda: aggregate_rdd(fixture), repeats)
        fixture.unpersist()
        records.append(
            {
                "rows": size,
                "dataframe_seconds": dataframe_seconds,
                "rdd_seconds": rdd_seconds,
                "speedup_rdd_over_dataframe": rdd_seconds / dataframe_seconds,
            }
        )
        print(
            f"[benchmark] rows={size:,} dataframe={dataframe_seconds:.2f}s "
            f"rdd={rdd_seconds:.2f}s "
            f"rdd/dataframe={rdd_seconds / dataframe_seconds:.2f}x"
        )

    payload = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "spark_version": spark.version,
        "master": spark.sparkContext.master,
        "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
        "repeats": repeats,
        "warmups": 1,
        "aggregation": "avg play duration + skip rate per genre",
        "records": records,
    }
    (destination / JSON_NAME).write_text(json.dumps(payload, indent=2), encoding="utf-8")
    write_chart(records, destination / CHART_NAME)
    print(f"[benchmark] wrote {destination / CHART_NAME}")
    print(f"[benchmark] wrote {destination / JSON_NAME}")
    return payload


def main(argv=None):
    parser = argparse.ArgumentParser(description="RDD vs DataFrame benchmark")
    parser.add_argument(
        "--quick",
        action="store_true",
        help="reduced sizes and a single repeat for a smoke run",
    )
    args = parser.parse_args(argv)

    cfg = load_config()
    sizes = (1_000, 10_000) if args.quick else SIZES
    repeats = 1 if args.quick else REPEATS
    spark = (
        SparkSession.builder.appName("RddVsDataFrameBenchmark")
        .master(cfg["spark"]["master"])
        .config("spark.sql.shuffle.partitions", "64")
        .getOrCreate()
    )
    try:
        print(
            f"[benchmark] spark={spark.version} master={spark.sparkContext.master} "
            f"shuffle_partitions={spark.conf.get('spark.sql.shuffle.partitions')} "
            f"sizes={list(sizes)} repeats={repeats}"
        )
        run_benchmark(spark, sizes=sizes, repeats=repeats)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
