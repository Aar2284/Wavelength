import os

os.environ["PYSPARK_PYTHON"] = r"C:\Users\aarya\anaconda3\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\aarya\anaconda3\python.exe"

from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "TestSpark", master: str = "local[1]") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.python.worker.reuse", "true")
        .getOrCreate()
    )


def test_rdd_operations(spark: SparkSession):
    sc = spark.sparkContext
    rdd = sc.parallelize([1, 2, 3, 4, 5])
    result = rdd.map(lambda x: x * x).collect()
    assert result == [1, 4, 9, 16, 25], f"Expected [1,4,9,16,25], got {result}"
    print(f"[PASS] RDD squares: {result}")


def test_dataframe_operations(spark: SparkSession):
    df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
    assert df.count() == 2
    assert df.columns == ["id", "name"]
    df.show()
    print("[PASS] DataFrame creation and show")


def test_sql_operations(spark: SparkSession):
    df = spark.createDataFrame(
        [(1, "Rock", 100), (2, "Jazz", 80), (3, "Rock", 120)],
        ["song_id", "genre", "plays"]
    )
    df.createOrReplaceTempView("songs")
    result = spark.sql(
        "SELECT genre, SUM(plays) as total_plays FROM songs GROUP BY genre ORDER BY total_plays DESC"
    ).collect()
    assert result[0]["genre"] == "Rock"
    print(f"[PASS] SQL aggregation: {result}")


if __name__ == "__main__":
    spark = get_spark_session("HelloSpark")

    test_rdd_operations(spark)
    test_dataframe_operations(spark)
    test_sql_operations(spark)

    spark.stop()
    print("\n=== All Spark sanity tests passed! ===")
