FROM apache/airflow:2.10.3-python3.10

USER root

# 1. Install dependencies dasar untuk Chrome
RUN apt-get update && apt-get install -y \
   wget \
   gnupg \
   unzip \
   libnss3 \
   libgconf-2-4 \
   libfontconfig1 \
   libglib2.0-0 \
   libnss3 \
   libgconf-2-4 \
   libfontconfig1 \
   && rm -rf /var/lib/apt/lists/*

# 2. Install Google Chrome (Stable)
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
   && sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list' \
   && apt-get update \
   && apt-get install -y google-chrome-stable

# 3. Kembali ke user airflow agar aman
USER airflow

# 4. copy requirements jika ada modul python tambahan
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# 5. install playwright untuk scraping
RUN playwright install chromium