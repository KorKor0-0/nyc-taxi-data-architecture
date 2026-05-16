"""
Data quality validation utilities.
"""

import logging
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.parquet as pq

log = logging.getLogger(__name__)

REQUIRED_COLUMNS = {
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_distance",
    "fare_amount",
    "total_amount",
    "passenger_count",
    "payment_type",
}

MAX_NULL_PCT = 1.0
MAX_NEG_FARE_PCT = 5.0
MIN_ROWS = 100_000

def validate_taxi_dataset(
    file_path: Path,
    year: int
) -> bool:
    """Run basic data quality checks."""

    log.info(
        "Running data quality validation"
    )

    table = pq.read_table(file_path)
    row_count = table.num_rows
    columns = set(table.schema.names)

    # Check schema
    missing_columns = (
        REQUIRED_COLUMNS - columns
    )

    if missing_columns:

        raise ValueError(
            f"Missing columns: "
            f"{sorted(missing_columns)}"
        )

    log.info("Schema validation passed")

    # Check row count
    if row_count == 0:

        raise ValueError(
            "Dataset is empty"
        )

    if row_count < MIN_ROWS:

        log.warning(
            "Low row count: %s",
            f"{row_count:,}"
        )

    log.info(
        "Row count: %s",
        f"{row_count:,}"
    )

    # Check null pickup timestamps
    null_count = pc.sum(
        pc.is_null(
            table["tpep_pickup_datetime"]
        )
    ).as_py()

    null_pct = (
        null_count / row_count * 100
    )

    if null_pct > MAX_NULL_PCT:

        raise ValueError(
            "Null pickup timestamp "
            f"exceeds threshold: "
            f"{null_pct:.2f}%"
        )

    log.info(
        "Null validation passed"
    )

    # Check negative fares
    negative_count = pc.sum(
        pc.less(
            table["fare_amount"],
            0
        )
    ).as_py()

    negative_pct = (
        negative_count / row_count * 100
    )

    if negative_pct > MAX_NEG_FARE_PCT:

        raise ValueError(
            "Negative fare percentage "
            f"exceeds threshold: "
            f"{negative_pct:.2f}%"
        )

    log.info(
        "Fare validation passed"
    )

    # Check date range
    min_date = pc.min(
        table["tpep_pickup_datetime"]
    ).as_py()

    if min_date.year != year:

        log.warning(
            "Dataset year mismatch: "
            "expected %s, found %s",
            year,
            min_date.year
        )

    log.info(
        "Date range validation passed"
    )

    log.info(
        "Data quality validation completed"
    )
    return True

if __name__ == "__main__":

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | "
            "%(levelname)s | "
            "%(message)s"
        )
    )

    validate_taxi_dataset(
        file_path=Path(
            "data/raw/2024/01/"
            "yellow_tripdata_2024-01.parquet"
        ),
        year=2024
    )