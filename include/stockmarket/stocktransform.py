# README:
# SPARK_APPLICATION_ARGS contains stock-market/AAPL/prices.json
# SPARK_APPLICATION_ARGS will be passed to the Spark application as an argument -e when running the Spark application from Airflow
# - Sometimes the script can stay stuck after "Passing arguments..."
# - Sometimes the script can stay stuck after "Successfully stopped SparkContext"
# - Sometimes the script can show "WARN TaskSchedulerImpl: Initial job has not accepted any resources; check your cluster UI to ensure that workers are registered and have sufficient resources"
# The easiest way to solve that is to restart your Airflow instance
# astro dev kill && astro dev start
# Also, make sure you allocated at least 8gb of RAM to Docker Desktop
# Go to Docker Desktop -> Preferences -> Resources -> Advanced -> Memory

# Import the SparkSession module
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import explode, arrays_zip, from_unixtime
from pyspark.sql.types import DateType
from dotenv import load_dotenv

load_dotenv()

import os
import sys



def app():
    # Create a SparkSession
    spark = SparkSession.builder.appName("FormatStock") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")) \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("ENDPOINT", "http://localhost:9000/")) \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.attempts.maximum", "1") \
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "10000") \
        .config("spark.jars", "/home/xaviera/Documents/udemy_airflow/venv/lib/python3.10/site-packages/pyspark/jars/hadoop-aws-3.3.4.jar,/home/xaviera/Documents/udemy_airflow/venv/lib/python3.10/site-packages/pyspark/jars/aws-java-sdk-bundle-1.12.262.jar")\
        .getOrCreate()


    # Read a JSON file from an MinIO bucket using the access key, secret key, 
    # and endpoint configured above
    df = spark.read.option("header", "false") \
        .json(f"s3a://{os.getenv('SPARK_APPLICATION_ARGS')}/prices.json")
    


    # Explode the necessary arrays
    df_exploded = df.select("timestamp", explode("indicators.quote").alias("quote")) \
        .select("timestamp", "quote.*")

    # Zip the arrays
    df_zipped = df_exploded.select(arrays_zip("timestamp", "close", "high", "low", "open", "volume").alias("zipped"))
    df_zipped = df_zipped.select(explode("zipped")).select("col.timestamp", "col.close", "col.high", "col.low", "col.open", "col.volume")
    df_zipped = df_zipped.withColumn('date', from_unixtime('timestamp').cast(DateType()))

    # Store in Minio
    df_zipped.write \
        .mode("overwrite") \
        .option("header", "true") \
        .option("delimiter", ",") \
        .csv(f"s3a://{os.getenv('SPARK_APPLICATION_ARGS')}/formatted_prices")
    
if __name__ == "__main__":
    app()


# os.system('kill %d' % os.getpid())