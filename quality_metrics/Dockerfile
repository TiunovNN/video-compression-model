FROM python:3.12-slim

WORKDIR /quality_metrics
RUN apt-get update \
    && apt-get install -y \
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
ADD . .
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

ENTRYPOINT ["celery", "-A", "worker", "worker", "-c", "1"]
