FROM python:3.12

WORKDIR /quality_metrics
RUN apt-get update \
    && apt-get install -y \
        nscd \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ADD --checksum=sha256:e7e7fb30477f717e6f55f9180a70386c62677ef8a4d4d1a5d948f4098aa3eb99 \
    --chmod=755 \
    https://storage.yandexcloud.net/tnn-datasets/ffmpeg7 /bin/ffmpeg
ADD https://storage.yandexcloud.net/tnn-datasets/model.tar.gz /usr/local/share/
ADD requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
ADD . .
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

ENTRYPOINT ["celery", "-A", "worker", "worker", "-c", "1"]
