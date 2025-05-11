import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

def get_spark_session(app_name="Flight Data"):
    """
    Create and return a SparkSession configured for S3 access.
    """
    load_dotenv()
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_REGION = os.getenv("AWS_REGION")
    S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2") \
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{AWS_REGION}.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    return spark, S3_BUCKET_NAME

def load_dataframes(spark, bucket):
    """
    Load flights, airlines, and airports CSVs from S3 into DataFrames.
    """
    flights_path = f"s3a://{bucket}/small_flights_df.csv"
    airlines_path = f"s3a://{bucket}/airlines.csv"
    airports_path = f"s3a://{bucket}/airports.csv"
    flights_df = spark.read.csv(flights_path, header=True, inferSchema=True)
    airlines_df = spark.read.csv(airlines_path, header=True, inferSchema=True)
    airports_df = spark.read.csv(airports_path, header=True, inferSchema=True)
    return flights_df, airlines_df, airports_df

if __name__ == "__main__":
    spark, bucket = get_spark_session()
    flights_df, airlines_df, airports_df = load_dataframes(spark, bucket)

    print("Flights Schema:")
    flights_df.printSchema()
    flights_df.show(5)

    print("Airlines Schema:")
    airlines_df.printSchema()
    airlines_df.show(5)

    print("Airports Schema:")
    airports_df.printSchema()
    airports_df.show(5)

    spark.stop()