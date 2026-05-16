FROM apache/airflow:2.9.1

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow
COPY reqs.txt /reqs.txt
RUN pip install --no-cache-dir -r /reqs.txt duckdb polars
