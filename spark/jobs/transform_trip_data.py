"""
NYC Taxi transformation job.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    hour,
    dayofweek,
    month,
    year,
    round,
    when
)

# Get arguments from command line
year_param = sys.argv[1]
month_param = sys.argv[2]

# Create Spark session
spark = (
    SparkSession.builder
    .appName("NYC Taxi Transformation")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Dynamic processed path
input_path = (
    f"data/processed/{year_param}/{month_param}/"
)

# Read processed parquet
df = spark.read.parquet(input_path)

print("\nProcessed row count:")

print(df.count())

# Transform dataset
df_transformed = (

    df

    # Date dimensions
    .withColumn(
        "pickup_year",
        year(
            col("tpep_pickup_datetime")
        )
    )

    .withColumn(
        "pickup_month",
        month(
            col("tpep_pickup_datetime")
        )
    )

    .withColumn(
        "pickup_hour",
        hour(
            col("tpep_pickup_datetime")
        )
    )

    .withColumn(
        "pickup_weekday",
        dayofweek(
            col("tpep_pickup_datetime")
        )
    )

    # Revenue efficiency
    .withColumn(
        "revenue_per_mile",
        round(
            col("fare_amount")
            /
            col("trip_distance"),
            2
        )
    )

    # Average speed
    .withColumn(
        "avg_speed_mph",
        round(

            when(
                col("trip_duration_minutes") > 0,

                (
                    col("trip_distance")
                    /
                    (
                        col("trip_duration_minutes")
                        / 60
                    )
                )
            )

            .otherwise(None),

            2
        )
    )
)

print("\nTransformed row count:")

print(df_transformed.count())

print("\nShowing transformed sample:")

df_transformed.select(
    "tpep_pickup_datetime",
    "fare_amount",
    "trip_distance",
    "trip_duration_minutes",
    "pickup_hour",
    "pickup_weekday",
    "revenue_per_mile",
    "avg_speed_mph"
).show(5)

# Dynamic curated path
output_path = (
    f"data/curated/{year_param}/{month_param}/"
)

(
    df_transformed
    .write
    .mode("overwrite")
    .parquet(output_path)
)

print(
    f"\nCurated parquet saved to:"
    f" {output_path}"
)

spark.catalog.clearCache()

# Stop Spark session
spark.stop()