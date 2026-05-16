FROM apache/airflow:2.8.1

USER root

RUN apt-get update && apt-get install -y \
    default-jdk \
    procps \
    && apt-get clean

USER airflow

RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    pandas==2.0.3 \
    pyarrow==12.0.1