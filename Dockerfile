FROM mcr.microsoft.com/playwright/python:v1.42.0-jammy

# Temel imajı seçtikten hemen sonra ekle
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y \
    xvfb \
    x11vnc \
    fluxbox \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
# Ekstra stealth eklentileri (bazen playwright install gerekebiliyor)
RUN playwright install chromium

COPY . /app

# Dosya yetkilerini ver
RUN chmod +x /app/start_worker.sh

EXPOSE 5900

CMD ["/app/start_worker.sh"]
