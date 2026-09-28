from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from src.common.config import load_config
from src.ingestion.events_stream import build_spark

ROOT = Path(__file__).resolve().parents[2]

EVENTS_VIEW = "events"
CATALOG_VIEW = "catalog"


def register_views(spark: SparkSession, events_df: DataFrame, catalog_df: DataFrame) -> None:
    events_df.createOrReplaceTempView(EVENTS_VIEW)
    catalog_df.createOrReplaceTempView(CATALOG_VIEW)


def read_store_views(spark: SparkSession, cfg: dict) -> None:
    events_df = spark.read.parquet(str(ROOT / cfg["paths"]["events_store"]))
    catalog_df = spark.read.format("delta").load(str(ROOT / cfg["paths"]["catalog_store"]))
    register_views(spark, events_df, catalog_df)


def avg_playcount_by_genre(spark: SparkSession) -> DataFrame:
    return spark.sql(
        """
        SELECT genre,
               AVG(playcount) AS avg_playcount
        FROM (
            SELECT genre, trackName, artistName, COUNT(*) AS playcount
            FROM events
            WHERE genre IS NOT NULL
            GROUP BY genre, trackName, artistName
        )
        GROUP BY genre
        ORDER BY avg_playcount DESC
        """
    )


def top_tracks_by_genre(spark: SparkSession, limit: int = 5) -> DataFrame:
    return spark.sql(
        f"""
        SELECT genre, rank, trackName, artistName, playcount
        FROM (
            SELECT genre,
                   trackName,
                   artistName,
                   COUNT(*) AS playcount,
                   RANK() OVER (PARTITION BY genre ORDER BY COUNT(*) DESC) AS rank
            FROM events
            WHERE genre IS NOT NULL
            GROUP BY genre, trackName, artistName
        )
        WHERE rank <= {int(limit)}
        ORDER BY genre, rank
        """
    )


def genre_release_join(spark: SparkSession) -> DataFrame:
    return spark.sql(
        """
        SELECT e.trackName,
               e.artistName,
               c.title,
               c.releaseDate,
               c.genre,
               g.releases_in_genre
        FROM events e
        JOIN catalog c
            ON lower(trim(e.artistName)) = lower(trim(c.artist))
            AND lower(trim(e.trackName)) = lower(trim(c.title))
        JOIN (
            SELECT genre, COUNT(*) AS releases_in_genre
            FROM catalog
            WHERE genre IS NOT NULL
            GROUP BY genre
        ) g
            ON c.genre = g.genre
        """
    )


def love_titles(spark: SparkSession) -> DataFrame:
    return spark.sql(
        """
        SELECT title, artist, genre, releaseDate
        FROM catalog
        WHERE LOWER(title) LIKE '%love%'
        ORDER BY releaseDate
        """
    )


def main(argv=None):
    cfg = load_config()
    events_path = ROOT / cfg["paths"]["events_store"]
    catalog_path = ROOT / cfg["paths"]["catalog_store"]
    if not events_path.exists() or not (catalog_path / "_delta_log").exists():
        raise SystemExit("[sql-analytics] stores missing - run the pollers and streaming queries first")
    spark = build_spark(cfg)
    try:
        read_store_views(spark, cfg)
        for label, frame in (
            ("avg playcount by genre", avg_playcount_by_genre(spark)),
            ("top tracks by genre (rank <= 5)", top_tracks_by_genre(spark)),
            ("events x catalog x genre totals join", genre_release_join(spark)),
            ("titles matching %love%", love_titles(spark)),
        ):
            print(f"[sql-analytics] {label}:")
            frame.show(20, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
