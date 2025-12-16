# Gunakan image resmi Airflow yang sudah dibundle dengan Python 3.10
FROM apache/airflow:2.7.1-python3.10

USER root
# (Sisa kode ke bawah tetap sama)
RUN apt-get update && apt-get install -y \
   build-essential \
   git \
   && apt-get clean

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt