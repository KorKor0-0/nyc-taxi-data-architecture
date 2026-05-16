"""
NYC Taxi ingestion job.
"""

import logging
import sys
from pathlib import Path

# Add project root to Python path

PROJECT_ROOT = (
    Path(__file__).resolve().parents[2]
)

sys.path.append(str(PROJECT_ROOT))

from dags.helpers.downloader import download

logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s | "
        "%(levelname)s | "
        "%(message)s"
    )
)

log = logging.getLogger(__name__)


def run_ingestion(
    year: int,
    month: int
) -> None:
    """Run ingestion pipeline."""

    log.info(
        "Starting ingestion pipeline"
    )

    file_path = download(
        data_dir="data/raw",
        year=year,
        month=month
    )

    log.info(
        "Ingestion completed"
    )

    log.info(
        "Dataset saved to: %s",
        file_path
    )


if __name__ == "__main__":

    # Get parameters from command line

    year = int(sys.argv[1])
    month = int(sys.argv[2])

    run_ingestion(
        year=year,
        month=month
    )