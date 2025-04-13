# video-compression-model

## S3 Uploader

Скрипт для перекачивания видео из одного источника в другой.

## Feature calculator

Код для подсчета различных характеристик исходного видео.

## Encoder

Код для кодирования видео. Для запуска задач на респределенном кластере
используется celery

1. Поднимаем базу и создаем таблицы
```shell
python manage.py create-table
```
2. Запускаем воркеры где нужно
```shell
docker-compose up -d
```
3. Создаем задачи в очереди
```shell
python manage.py generate-tasks --qp-min 25 --qp-max 40 --crf-min 17 --crf-max 30 --input-bucket ${bucket}
```

## Quality metrics

Код для подсчета качества видео на основе метрики MS-SSIM

1. Запускаем воркеры
```shell
docker-compose up -d
```
2. Создаем задачи в очереди
```shell
python manage.py generate-tasks
```
