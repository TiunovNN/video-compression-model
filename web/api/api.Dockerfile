FROM python:3.12-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y libmagic1 libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ADD requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
ADD src /app
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

EXPOSE 80
# Command to run the application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80", "--log-config=log_conf.yaml"]
