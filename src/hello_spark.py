import os
os.environ["PYSPARK_PYTHON"] = r"C:\Users\aarya\anaconda3\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\aarya\anaconda3\python.exe"

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HelloSpark") \
    .master("local[1]") \
    .config("spark.python.worker.reuse", "true") \
    .getOrCreate()

sc = spark.sparkContext

rdd = sc.parallelize([1, 2, 3, 4, 5])
result = rdd.map(lambda x: x * x).collect()
print(f"Squares: {result}")

df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
df.show()

spark.stop()
print("Spark sanity test passed!")
