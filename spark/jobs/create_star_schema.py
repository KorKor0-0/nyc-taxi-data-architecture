"""
Create Star Schema tables from curated NYC Taxi data.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    month,
    hour,
    dayofweek,
    when,
    col
)

# Get Parameters

year_param = sys.argv[1]
month_param = sys.argv[2]

# Create Spark Session

spark = (
    SparkSession.builder
    .appName("NYC Taxi Star Schema")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Read curated parquet

input_path = (
    f"data/curated/{year_param}/{month_param}/"
)

df = spark.read.parquet(input_path)

print("\nCurated schema:")
df.printSchema()

# Create Dimension Tables

# DIM DATE

dim_date = (
    df
    .select(
        col("tpep_pickup_datetime").alias("pickup_datetime")
    )
    .dropDuplicates()
    .withColumn(
        "pickup_month",
        month("pickup_datetime")
    )
    .withColumn(
        "pickup_hour",
        hour("pickup_datetime")
    )
    .withColumn(
        "pickup_weekday",
        dayofweek("pickup_datetime")
    )
)

# DIM PAYMENT

dim_payment = (
    df
    .select("payment_type")
    .dropDuplicates()
)

# DIM VENDOR

dim_vendor = (
    df
    .select(
        col("VendorID").alias("vendor_id")
    )
    .dropDuplicates()
    .withColumn(
        "vendor_name",
        when(col("vendor_id") == 1, "Creative Mobile Technologies")
        .when(col("vendor_id") == 2, "VeriFone Inc")
        .otherwise("Unknown")
    )
)

# DIM LOCATION

dim_location = (
    df
    .select(
        col("PULocationID").alias("pickup_location_id"),
        col("DOLocationID").alias("dropoff_location_id")
    )
    .dropDuplicates()
)

# Create Fact Table

fact_trip = (
    df
    .select(
        "VendorID",
        "payment_type",
        "PULocationID",
        "DOLocationID",
        "pickup_month",
        "pickup_hour",
        "pickup_weekday",
        "passenger_count",
        "trip_distance",
        "trip_duration_minutes",
        "fare_amount",
        "tip_amount",
        "total_amount"
    )
)

# Save Warehouse Tables

# Append for dimensions

(
    dim_date
    .write
    .mode("append")
    .parquet("data/warehouse/dim_date/")
)

(
    dim_payment
    .write
    .mode("append")
    .parquet("data/warehouse/dim_payment/")
)

(
    dim_vendor
    .write
    .mode("append")
    .parquet("data/warehouse/dim_vendor/")
)

(
    dim_location
    .write
    .mode("append")
    .parquet("data/warehouse/dim_location/")
)

# Fact table partitioned by month

(
    fact_trip
    .write
    .mode("overwrite")
    .parquet(
        f"data/warehouse/fact_trip/{year_param}/{month_param}/"
    )
)

print("\nStar Schema created successfully.")

spark.stop()