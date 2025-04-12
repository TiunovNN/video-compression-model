import csv
import io
import logging
import os
import sys
from functools import cached_property
from urllib.parse import urlparse

import boto3
from celery import Celery, Task
from dotenv import load_dotenv
from sqlalchemy import URL

from decoder import Decoder
from extractors import (Extractor, UExtractor, VExtractor, YExtractor)
from metrics import (
    MSSSIMCalculator,
    MetricCalculator,
)

Processor = Extractor | MetricCalculator


def analyze_file(source_url, distorted_url) -> bytes:
    extractors = [
        YExtractor(),
        UExtractor(),
        VExtractor(),
    ]
    extractors = {
        extractor.name(): extractor
        for extractor in extractors
    }
    metric_calculator = [
        MSSSIMCalculator('Y'),
        MSSSIMCalculator('U'),
        MSSSIMCalculator('V'),
    ]
    buffer = io.StringIO()
    fieldnames = [
        'source_format',
        'distorted_format',
        'source_key_frame',
        'distorted_key_frame',
        'source_time',
        'distorted_time',
        'source_pts',
        'distorted_pts',
    ]
    for calculator in metric_calculator:
        fieldnames.append(calculator.name())

    writer = csv.DictWriter(buffer, fieldnames=fieldnames, quoting=csv.QUOTE_STRINGS, delimiter='|')
    writer.writeheader()
    with Decoder(source_url) as decoder_source, Decoder(distorted_url) as decoder_distorted:
        for frame_source, frame_distorted in zip(decoder_source, decoder_distorted):
            item = {
                'source_format': frame_source.format.name,
                'distorted_format': frame_distorted.format.name,
                'source_key_frame': int(frame_source.key_frame),
                'distorted_key_frame': int(frame_distorted.key_frame),
                'source_time': frame_source.time,
                'distorted_time': frame_distorted.time,
                'source_pts': frame_source.pts,
                'distorted_pts': frame_source.pts,
            }
            source_array = frame_source.to_ndarray()
            distorted_array = frame_distorted.to_ndarray()
            for calculator in metric_calculator:
                extractor = extractors[calculator.depends_on()]
                source_image = extractor.extract(source_array)
                distorted_image = extractor.extract(distorted_array)
                value = calculator.feed_frame(source_image, distorted_image)
                item[calculator.name()] = value

            writer.writerow(item)
    return buffer.getvalue().encode()


class QualityAnalyzeTask(Task):
    name = 'quality_analyze'

    @cached_property
    def s3_client(self):
        s3_access_key_id = self.app.conf.get('S3_ACCESS_KEY_ID')
        s3_secret_access_key = self.app.conf.get('S3_SECRET_ACCESS_KEY')
        s3_endpoint_url = self.app.conf.get('S3_ENDPOINT_URL')

        if not all([s3_access_key_id, s3_secret_access_key]):
            raise ValueError("S3 credentials not configured in worker")

        return boto3.client(
            's3',
            endpoint_url=s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key,
        )

    @property
    def output_bucket(self):
        return self.app.conf.get('s3_output_bucket')

    def run(self, source_url: str, distorted_url: str):
        parsed_source = urlparse(source_url)
        presigned_source_url = self.s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': parsed_source.netloc, 'Key': parsed_source.path.lstrip('/')},
            ExpiresIn=3600 * 24,
        )
        parsed_distorted = urlparse(distorted_url)
        presigned_distorted_url = self.s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': parsed_distorted.netloc, 'Key': parsed_distorted.path.lstrip('/')},
            ExpiresIn=3600 * 24,
        )
        logging.info(f'Analyzing file {distorted_url}')
        csv_data = analyze_file(presigned_source_url, presigned_distorted_url)
        logging.info(
            f'Uploading {len(csv_data)} to {self.output_bucket}/{parsed_distorted.path}.csv'
        )
        self.s3_client.put_object(
            Bucket=self.output_bucket,
            Key=f'{parsed_distorted.path}.csv',
            Body=csv_data,
            Metadata={'Content-Type': 'text/csv'}
        )


def configure_logging():
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setLevel(logging.INFO)
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s\n',
        handlers=[handler],
        force=True,
    )


def configure_celery():
    app = Celery('encoder_tasks')

    result_backend = URL.create(
        drivername='db+' + os.getenv('DATABASE_DRIVER'),
        host=os.getenv('DATABASE_HOST'),
        username=os.getenv('DATABASE_USER'),
        password=os.getenv('DATABASE_PASSWORD'),
        database=os.getenv('DATABASE_NAME'),
        port=os.getenv('DATABASE_PORT'),
    ).render_as_string(hide_password=False)
    print(f'{result_backend=}')
    # Глобальные настройки, которые будут использоваться в Celery worker
    app.conf.update(
        broker_url=os.getenv('CELERY_BROKER_URL', 'redis://localhost/0'),
        result_backend=os.getenv('CELERY_RESULT_BACKEND', result_backend),
        s3_endpoint_url=os.getenv('S3_ENDPOINT_URL', 'https://storage.yandexcloud.net/'),
        s3_access_key_id=os.getenv('S3_ACCESS_KEY_ID'),
        s3_secret_access_key=os.getenv('S3_SECRET_ACCESS_KEY'),
        s3_output_bucket=os.getenv('S3_OUTPUT_BUCKET'),
        database_driver=os.getenv('DATABASE_DRIVER'),
        database_host=os.getenv('DATABASE_HOST'),
        database_user=os.getenv('DATABASE_USER'),
        database_password=os.getenv('DATABASE_PASSWORD'),
        database_name=os.getenv('DATABASE_NAME'),
        database_port=os.getenv('DATABASE_PORT'),
        task_default_queue='quality_analyze'
    )
    app.conf.broker_transport_options = {'is_secure': True}

    return app


load_dotenv()
configure_logging()
celery = configure_celery()
quality_analyze_task = celery.register_task(QualityAnalyzeTask)
