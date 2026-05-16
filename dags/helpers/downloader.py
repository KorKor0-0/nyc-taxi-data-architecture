"""
Reusable downloader utility for NYC TLC parquet datasets.
"""

import logging
from pathlib import Path

import requests

log = logging.getLogger(__name__)

TLC_URL_TEMPLATE = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    "yellow_tripdata_{year}-{month:02d}.parquet"
)

CHUNK_SIZE = 8 * 1024 * 1024


def build_url(year: int, month: int) -> str:
    """Build NYC TLC dataset URL."""

    return TLC_URL_TEMPLATE.format(
        year=year,
        month=month
    )


def build_local_path(
    data_dir: str,
    year: int,
    month: int
) -> Path:
    """Build local parquet storage path."""

    folder = (
        Path(data_dir)
        / str(year)
        / f"{month:02d}"
    )

    folder.mkdir(
        parents=True,
        exist_ok=True
    )

    return folder / (
        f"yellow_tripdata_{year}-{month:02d}.parquet"
    )


def file_exists(
    data_dir: str,
    year: int,
    month: int
) -> bool:
    """Check whether parquet file already exists."""

    path = build_local_path(
        data_dir=data_dir,
        year=year,
        month=month
    )

    return (
        path.exists()
        and path.stat().st_size > 0
    )


def download(
    data_dir: str,
    year: int,
    month: int
) -> Path:
    """Download parquet dataset from NYC TLC CDN."""

    url = build_url(year, month)

    save_path = build_local_path(
        data_dir=data_dir,
        year=year,
        month=month
    )

    if file_exists(data_dir, year, month):

        log.info(
            "File already exists: %s",
            save_path
        )

        return save_path

    log.info("Starting dataset download")
    log.info("URL: %s", url)
    log.info("Output: %s", save_path)

    with requests.get(
        url,
        stream=True,
        timeout=300
    ) as response:

        response.raise_for_status()

        total_bytes = int(
            response.headers.get(
                "content-length",
                0
            )
        )

        downloaded = 0
        last_logged_percent = -10

        with open(save_path, "wb") as file:

            for chunk in response.iter_content(
                chunk_size=CHUNK_SIZE
            ):

                if not chunk:
                    continue

                file.write(chunk)

                downloaded += len(chunk)

                if total_bytes > 0:

                    percent = (
                        downloaded
                        / total_bytes
                        * 100
                    )

                    if (
                        percent
                        - last_logged_percent
                        >= 10
                    ):

                        log.info(
                            "Progress: %.0f%% "
                            "(%.1f MB / %.1f MB)",
                            percent,
                            downloaded / 1e6,
                            total_bytes / 1e6,
                        )

                        last_logged_percent = percent

    final_size_mb = (
        save_path.stat().st_size / 1e6
    )

    log.info("Download completed")
    log.info(
        "File size: %.1f MB",
        final_size_mb
    )

    return save_path


if __name__ == "__main__":

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | "
            "%(levelname)s | "
            "%(message)s"
        )
    )

    download(
        data_dir="data/raw",
        year=2024,
        month=1
    )