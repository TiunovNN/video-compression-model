
FROM python:3.12-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ADD --chmod=755 https://storage.yandexcloud.net/tnn-datasets/ffmpeg /bin/ffmpeg
ADD requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
ADD src /app
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

CMD ["celery", "-A", "celery_worker", "worker", "-c", "1", "--loglevel", "info"]
