# Gunakan 'bookworm' agar stabil dan paket linux lengkap
FROM python:3.10-slim-bookworm

WORKDIR /app

# (Sisa kode ke bawah tetap sama)
RUN apt-get update && apt-get install -y \
   build-essential \
   curl \
   git \
   && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip3 install -r requirements.txt

EXPOSE 8501

CMD ["streamlit", "run", "streamlit_app/main.py"]