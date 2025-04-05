FROM python:3.12-slim

WORKDIR /encoder
RUN apt update \
    && apt install -y ffmpeg libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ADD requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
ADD . .
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

ENTRYPOINT ["celery", "-A", "worker", "worker", "-c", "1"]
