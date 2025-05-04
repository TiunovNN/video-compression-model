FROM python:3.12-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y \
        ffmpeg \
        libpq-dev \
        libavcodec-dev \
        libavdevice-dev \
        libavfilter-dev \
        libavformat-dev \
        libavutil-dev \
        libswresample-dev \
        libswscale-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ADD requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
ADD --checksum=sha256:a031e5eb15e0399f6bbf14ab28970a9e5e82c2fa171539c878abbf8b967eb6a2 \
    https://storage.yandexcloud.net/tnn-datasets/model_v2.cbm /app/model.cbm
ADD src /app
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

CMD ["celery", "-A", "celery_worker", "worker", "-c", "1", "--loglevel", "info"]
