FROM apache/airflow:2.9.2-python3.11

# keep pip lean
ENV PIP_NO_CACHE_DIR=1

# (optional) useful system tools
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    git curl \
 && rm -rf /var/lib/apt/lists/*
USER airflow

# install python deps (dbt)
COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip \
 && pip install --no-cache-dir -r /requirements.txt

# ensure the dbt CLI is on PATH for BashOperator
ENV PATH="/home/airflow/.local/bin:${PATH}"