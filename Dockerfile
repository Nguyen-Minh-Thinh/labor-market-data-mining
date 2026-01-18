FROM apache/airflow:slim-latest-python3.11

# Default directory to work in docker container
WORKDIR /opt/airflow

# Copy requirements.txt file from local host to default directory of container
COPY requirements.txt .

USER root

RUN apt-get update && apt-get install -y \
    chromium \
    chromium-driver \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install -r requirements.txt