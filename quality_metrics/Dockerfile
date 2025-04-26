FROM python:3.12

WORKDIR /quality_metrics
ADD --checksum=sha256:49cbcdfce49317f7a96ff07b143d0f7e8f1bab088c3da1932832e9403dea965e \
    --chmod=755 \
    https://storage.yandexcloud.net/tnn-datasets/ffmpeg /bin/ffmpeg
ADD https://storage.yandexcloud.net/tnn-datasets/model.tar.gz /usr/local/share/
ADD requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
ADD . .
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

ENTRYPOINT ["celery", "-A", "worker", "worker", "-c", "1", "--loglevel", "info"]
