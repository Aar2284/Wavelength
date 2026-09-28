import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import pytest
from pyspark.sql import SparkSession

from src.sql_analytics.queries import (
    avg_playcount_by_genre,
    genre_release_join,
    love_titles,
    register_views,
    top_tracks_by_genre,
)

EVENTS_ROWS = [
    ("alice", "Bohemian", "Queen", "rock", "2026-11-01T10:00:00+00:00"),
    ("alice", "Bohemian", "Queen", "rock", "2026-11-01T11:00:00+00:00"),
    ("alice", "Bohemian", "Queen", "rock", "2026-11-01T12:00:00+00:00"),
    ("bob", "Yellow", "Coldplay", "rock", "2026-11-02T10:00:00+00:00"),
    ("carol", "So What", "Miles", "jazz", "2026-11-03T10:00:00+00:00"),
    ("carol", "So What", "Miles", "jazz", "2026-11-03T11:00:00+00:00"),
    ("carol", "So What", "Miles", "jazz", "2026-11-03T12:00:00+00:00"),
    ("carol", "So What", "Miles", "jazz", "2026-11-03T13:00:00+00:00"),
    ("carol", "So What", "Miles", "jazz", "2026-11-03T14:00:00+00:00"),
    ("dave", "Love in the Dark", "Adele", None, "2026-11-04T10:00:00+00:00"),
]

CATALOG_ROWS = [
    ("rel-1", "A Night at the Opera", "Queen", "1975-11-21", "rock"),
    ("rel-2", "Love in the Dark", "Adele", "2015-11-20", "pop"),
    ("rel-3", "Gloves", "The Hands", "2020-01-01", "pop"),
    ("rel-4", "My Love Song", "The Hands", "2021-01-01", None),
]


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.appName("SqlAnalyticsTests")
        .master("local[1]")
        .getOrCreate()
    )
    session.conf.set("spark.sql.shuffle.partitions", "4")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def views(spark):
    events_df = spark.createDataFrame(
        EVENTS_ROWS, "userId string, trackName string, artistName string, genre string, eventTime string"
    )
    catalog_df = spark.createDataFrame(
        CATALOG_ROWS, "releaseId string, title string, artist string, releaseDate string, genre string"
    )
    register_views(spark, events_df, catalog_df)
    return spark


def test_avg_playcount_by_genre_orders_desc(views):
    rows = avg_playcount_by_genre(views).collect()
    assert [row.genre for row in rows] == ["jazz", "rock"]
    assert rows[0].avg_playcount == pytest.approx(5.0)
    assert rows[1].avg_playcount == pytest.approx(2.0)


def test_top_tracks_by_genre_ranks_within_genre(views):
    rows = top_tracks_by_genre(views, limit=2).collect()
    jazz = [row for row in rows if row.genre == "jazz"]
    rock = [row for row in rows if row.genre == "rock"]
    assert [(row.rank, row.playcount) for row in jazz] == [(1, 5)]
    assert [(row.rank, row.trackName) for row in rock] == [
        (1, "Bohemian"),
        (2, "Yellow"),
    ]


def test_genre_release_join_matches_three_relations(views):
    rows = genre_release_join(views).collect()
    assert len(rows) == 1
    row = rows[0]
    assert row.trackName == "Love in the Dark"
    assert row.artistName == "Adele"
    assert row.title == "Love in the Dark"
    assert row.genre == "pop"
    assert row.releases_in_genre == 2


def test_love_titles_wildcard(views):
    titles = [row.title for row in love_titles(views).collect()]
    assert titles == ["Love in the Dark", "Gloves", "My Love Song"]
