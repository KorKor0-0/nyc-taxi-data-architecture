"""
NYC Taxi cleaning job.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    unix_timestamp,
    year,
    month
)

# Get parameters from command line

year_param = sys.argv[1]
month_param = sys.argv[2]

# Create Spark session

spark = (
    SparkSession.builder
    .appName("NYC Taxi Cleaning")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Read raw parquet

input_path = (
    f"data/raw/{year_param}/{month_param}/"
    f"yellow_tripdata_{year_param}-{month_param}.parquet"
)

df = spark.read.parquet(input_path)

print("\nRaw row count:")
print(df.count())

# Data cleaning

df_cleaned = (
    df
    .filter(col("fare_amount") > 0)

    .filter(col("trip_distance") > 0)

    .filter(
        col("tpep_pickup_datetime")
        .isNotNull()
    )

    .filter(
        col("tpep_dropoff_datetime")
        .isNotNull()
    )

    .filter(
        col("tpep_dropoff_datetime")
        >
        col("tpep_pickup_datetime")
    )

    # Filter correct year

    .filter(
        year(
            col("tpep_pickup_datetime")
        )
        == int(year_param)
    )

    # Filter correct month
    # ป้องกัน dirty data ข้ามเดือน

    .filter(
        month(
            col("tpep_pickup_datetime")
        )
        == int(month_param)
    )

    # Trip duration

    .withColumn(
        "trip_duration_minutes",

        (
            unix_timestamp(
                col("tpep_dropoff_datetime")
            )

            -

            unix_timestamp(
                col("tpep_pickup_datetime")
            )

        ) / 60
    )
)

print("\nCleaned row count:")
print(df_cleaned.count())

print("\nShowing cleaned sample:")

df_cleaned.select(
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "fare_amount",
    "trip_distance",
    "trip_duration_minutes"
).show(5)

# Dynamic output path

output_path = (
    f"data/processed/"
    f"{year_param}/{month_param}/"
)

(
    df_cleaned
    .write
    .mode("overwrite")
    .parquet(output_path)
)

print(
    f"\nProcessed parquet saved to:"
    f" {output_path}"
)

spark.catalog.clearCache()

# Stop Spark session

spark.stop()