FROM apache/airflow:slim-latest-python3.11

# Default directory to work in docker container
WORKDIR /opt/airflow

# Copy requirements.txt file from local host to default directory of container
COPY requirements.txt .

USER root

RUN apt-get update && apt-get install -y \
    chromium \
    chromium-driver \
    gcc \
    g++ \
    python3-dev \
    libffi-dev \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN cp /usr/bin/chromedriver /opt/airflow/chromedriver \
    && chown airflow /opt/airflow/chromedriver \
    && chmod 755 /opt/airflow/chromedriver

USER airflow
RUN pip install -r requirements.txt