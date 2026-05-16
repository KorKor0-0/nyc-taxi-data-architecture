"""
Load NYC Taxi Star Schema tables into PostgreSQL.
"""

from pyspark.sql import SparkSession

# Create Spark session
spark = (
    SparkSession.builder
    .appName("NYC Taxi PostgreSQL Load")
    .config(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.7.3"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# =========================
# Read warehouse parquet
# =========================

# รองรับหลายเดือนแบบ dynamic
fact_trip = (
    spark.read
    .option("recursiveFileLookup", "true")
    .parquet("data/warehouse/fact_trip/")
)

dim_date = (
    spark.read
    .option("recursiveFileLookup", "true")
    .parquet("data/warehouse/dim_date/")
)

dim_payment = (
    spark.read
    .option("recursiveFileLookup", "true")
    .parquet("data/warehouse/dim_payment/")
)

dim_vendor = (
    spark.read
    .option("recursiveFileLookup", "true")
    .parquet("data/warehouse/dim_vendor/")
)

dim_location = (
    spark.read
    .option("recursiveFileLookup", "true")
    .parquet("data/warehouse/dim_location/")
)

# =========================
# PostgreSQL connection
# =========================

# IMPORTANT:
# ใช้ postgres ไม่ใช่ localhost
# เพราะรันอยู่ใน Docker container

postgres_url = (
    "jdbc:postgresql://postgres:5432/nyc_taxi_dw"
)

postgres_properties = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

# =========================
# Load Dimension Tables
# =========================

print("\nLoading dim_date...")

(
    dim_date
    .dropDuplicates()
    .write.jdbc(
        url=postgres_url,
        table="public.dim_date",
        mode="overwrite",
        properties=postgres_properties
    )
)

print("dim_date loaded successfully.")

print("\nLoading dim_payment...")

(
    dim_payment
    .dropDuplicates()
    .write.jdbc(
        url=postgres_url,
        table="public.dim_payment",
        mode="overwrite",
        properties=postgres_properties
    )
)

print("dim_payment loaded successfully.")

print("\nLoading dim_vendor...")

(
    dim_vendor
    .dropDuplicates()
    .write.jdbc(
        url=postgres_url,
        table="public.dim_vendor",
        mode="overwrite",
        properties=postgres_properties
    )
)

print("dim_vendor loaded successfully.")

print("\nLoading dim_location...")

(
    dim_location
    .dropDuplicates()
    .write.jdbc(
        url=postgres_url,
        table="public.dim_location",
        mode="overwrite",
        properties=postgres_properties
    )
)

print("dim_location loaded successfully.")

# =========================
# Load Fact Table
# =========================

print("\nLoading fact_trip...")

# IMPORTANT:
# append เพื่อเก็บหลายเดือน
# ไม่ overwrite เดือนเก่า

(
    fact_trip
    .write.jdbc(
        url=postgres_url,
        table="public.fact_trip",
        mode="overwrite",
        properties=postgres_properties
    )
)

print("fact_trip loaded successfully.")

print("\nAll Star Schema tables loaded successfully.")

spark.catalog.clearCache()

# Stop Spark session
spark.stop()