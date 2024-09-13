# test_spark.py
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("TestSpark") \
        .getOrCreate()

    print("Spark session started successfully!")

    # Test data frame creation
    df = spark.createDataFrame([(1, "foo"), (2, "bar")], ["id", "value"])
    df.show()

    spark.stop()

if __name__ == "__main__":
    main()
