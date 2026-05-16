from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1)
}

# Process multiple months
YEAR = "2024"
MONTHS = ["01", "02"]

with DAG(
    dag_id="nyc_taxi_pipeline",
    default_args=default_args,

    # Manual trigger
    schedule_interval=None,

    catchup=False,

    tags=["spark", "postgres", "nyc-taxi"]
) as dag:

    months_str = " ".join(MONTHS)

    ingest_task = BashOperator(
        task_id="ingest_trip_data",
        bash_command=(
            f"cd /opt/airflow && "
            f"for month in {months_str}; do "
            f"python spark/jobs/ingest_trip_data.py {YEAR} $month; "
            f"done"
        )
    )

    clean_task = BashOperator(
        task_id="clean_trip_data",
        bash_command=(
            f"cd /opt/airflow && "
            f"for month in {months_str}; do "
            f"python spark/jobs/clean_trip_data.py {YEAR} $month; "
            f"done"
        )
    )

    transform_task = BashOperator(
        task_id="transform_trip_data",
        bash_command=(
            f"cd /opt/airflow && "
            f"for month in {months_str}; do "
            f"python spark/jobs/transform_trip_data.py {YEAR} $month; "
            f"done"
        )
    )

    star_schema_task = BashOperator(
        task_id="create_star_schema",
        bash_command=(
            f"cd /opt/airflow && "
            f"for month in {months_str}; do "
            f"python spark/jobs/create_star_schema.py {YEAR} $month; "
            f"done"
        )
    )

    load_postgres_task = BashOperator(
        task_id="load_to_postgres",
        bash_command=(
            "cd /opt/airflow && "
            "python spark/jobs/load_to_postgres.py"
        )
    )

    ingest_task >> clean_task >> transform_task >> star_schema_task >> load_postgres_task