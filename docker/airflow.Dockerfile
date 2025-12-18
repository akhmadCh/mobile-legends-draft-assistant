FROM apache/airflow:2.7.1-python3.10

USER root
RUN apt-get update && apt-get install -y \
   build-essential \
   git \
   && apt-get clean

USER airflow

COPY requirements.txt /requirements.txt

RUN pip install --upgrade pip && \
   pip install --default-timeout=1000 --no-cache-dir -r /requirements.txt