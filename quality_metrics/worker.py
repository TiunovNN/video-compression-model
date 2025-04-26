import logging
import os
import shutil
import subprocess
import sys
from functools import cached_property
from tempfile import NamedTemporaryFile
from typing import Optional
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError
from celery import Celery, Task
from dotenv import load_dotenv
from sqlalchemy import URL


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

    @cached_property
    def ffmpeg_bin(self) -> Optional[str]:
        executable_name = 'ffmpeg'
        possible_bin_dirs = [
            f'./{executable_name}',
            executable_name,
        ]

        for bin_ in possible_bin_dirs:
            if resolved_binary := shutil.which(bin_):
                return resolved_binary

        raise FileNotFoundError(f"{executable_name} not found in PATH")

    def analyze_file(self, source_url, distorted_url) -> bytes:
        with NamedTemporaryFile(suffix='.csv') as output_file:
            try:
                # Build the ffmpeg command
                input_params = [
                    '-seekable', '1',
                    '-reconnect_delay_max', '300',
                    '-multiple_requests', '1',
                    '-reconnect_on_http_error', '429,5xx',
                    '-reconnect_on_network_error', '1',
                    '-i', distorted_url,
                    '-seekable', '1',
                    '-reconnect_delay_max', '300',
                    '-multiple_requests', '1',
                    '-reconnect_on_http_error', '429,5xx',
                    '-reconnect_on_network_error', '1',
                    '-i', source_url,
                ]
                filter_params = [
                    '-lavfi', "libvmaf='"
                              r"model=version=vmaf_v0.6.1neg\:name=vmaf_neg"
                              f":n_threads={self.app.conf.get('thread_numbers')}"
                              ":log_fmt=csv"
                              f":log_path={output_file.name}'"
                ]
                global_params = [
                    '-f',
                    'null',
                    '-y',
                    '-hide_banner',
                    '-loglevel', 'error',
                    '-'
                ]

                # Run the command
                subprocess.run(
                    [
                        self.ffmpeg_bin,
                        *input_params,
                        *filter_params,
                        *global_params
                    ], check=True, capture_output=True, text=True
                )
            except subprocess.CalledProcessError as e:
                logging.error(f"Error encoding video: {e.stderr}")
                raise RuntimeError(str(e)) from e
            except Exception as e:
                logging.error(f"Unexpected error during encoding: {str(e)}")
                raise RuntimeError(f'Unknown error {e}') from e
            output_file.seek(0)
            return output_file.read()

    def run(self, source_url: str, distorted_url: str):
        parsed_source = urlparse(source_url)
        presigned_source_url = self.s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': parsed_source.netloc, 'Key': parsed_source.path.lstrip('/')},
            ExpiresIn=3600 * 24,
        )
        parsed_distorted = urlparse(distorted_url)
        new_path = parsed_distorted.path.lstrip('/') + '.vmaf.csv'
        try:
            self.s3_client.head_object(
                Bucket=self.output_bucket,
                Key=new_path,
            )
            logging.info(f'File {distorted_url} already analyzed')
            return
        except ClientError:
            pass

        presigned_distorted_url = self.s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': parsed_distorted.netloc, 'Key': parsed_distorted.path.lstrip('/')},
            ExpiresIn=3600 * 24,
        )
        logging.info(f'Analyzing file {distorted_url}')
        csv_data = self.analyze_file(presigned_source_url, presigned_distorted_url)
        logging.info(
            f'Uploading {len(csv_data)} to {self.output_bucket}/{new_path}'
        )
        self.s3_client.put_object(
            Bucket=self.output_bucket,
            Key=new_path,
            Body=csv_data,
            Metadata={'Content-Type': 'text/csv'},
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
        task_default_queue='quality_analyze',
        thread_numbers=os.getenv('THREAD_COUNT', os.cpu_count())
    )
    app.conf.broker_transport_options = {'is_secure': True}

    return app


load_dotenv()
configure_logging()
celery = configure_celery()
quality_analyze_task = celery.register_task(QualityAnalyzeTask)
