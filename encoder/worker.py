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
from celery import Celery, Task
from dotenv import load_dotenv
from sqlalchemy import URL, create_engine
from sqlalchemy.orm import sessionmaker

from models import EncoderTask, Status


class TranscodeVideoTask(Task):
    name = 'transcode_video'

    @cached_property
    def session_maker(self):
        url = URL.create(
            drivername=self.app.conf.get('database_driver'),
            host=self.app.conf.get('database_host'),
            username=self.app.conf.get('database_user'),
            password=self.app.conf.get('database_password'),
            port=self.app.conf.get('database_port'),
            database=self.app.conf.get('database_name')
        ).render_as_string(hide_password=False)
        engine = create_engine(url)
        return sessionmaker(engine, expire_on_commit=False)

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

    @cached_property
    def ffmpeg_bin(self) -> Optional[str]:
        executable_name = 'ffmpeg'
        possible_bin_dirs = [
            f'./{executable_name}',
            # Executable can be in same dir when graph build in "UNION" with required binaries
            executable_name,
        ]

        for bin_ in possible_bin_dirs:
            if resolved_binary := shutil.which(bin_):
                return resolved_binary

    @property
    def output_bucket(self):
        return self.app.conf.get('s3_output_bucket')

    def encode_video(
        self,
        input_path: str,
        output_path: str,
        crf: Optional[int],
        qp: Optional[int],
    ):
        """Encode a video using ffmpeg with the given parameters."""
        try:
            logging.info(f"Encoding video with CRF={crf}, QP={qp}")

            # Build the ffmpeg command
            input_params = [
                '-seekable', '1',
                '-reconnect_delay_max', '300',
                '-multiple_requests', '1',
                '-reconnect_on_http_error', '429,5xx',
                '-reconnect_on_network_error', '1',
                '-i', input_path,
            ]
            encode_params = [
                '-c:v', 'libx265',
                '-preset', 'veryslow',
            ]
            if crf:
                encode_params.append('-crf')
                encode_params.append(str(crf))

            else:
                encode_params.append('-qp')
                encode_params.append(str(qp))
            global_params = [
                '-an',
                '-sn',
                '-y',
                '-hide_banner',
                '-loglevel', 'error',
                output_path
            ]

            # Run the command
            subprocess.run(
                [
                    self.ffmpeg_bin,
                    *input_params,
                    *encode_params,
                    *global_params
                ], check=True, capture_output=True, text=True
            )
        except subprocess.CalledProcessError as e:
            logging.error(f"Error encoding video: {e.stderr}")
            raise RuntimeError(str(e)) from e
        except Exception as e:
            logging.error(f"Unexpected error during encoding: {str(e)}")
            raise RuntimeError(f'Unknown error {e}') from e

    def run(self, source_url: str, crf: Optional[int] = None, qp: Optional[int] = None):
        with self.session_maker.begin() as session:
            task = session.query(EncoderTask).filter_by(
                source_url=source_url,
                crf=crf,
                qp=qp,
            ).first()

            if not task:
                parsed_source_url = urlparse(source_url)
                prefix, _, filename = parsed_source_url.path.lstrip('/').rpartition('/')
                base_name, _, ext = filename.rpartition('.')
                task = EncoderTask(
                    source_url=source_url,
                    status=Status.ENQUEUED,
                )
                if crf:
                    task.crf = crf
                    task.destination_url = f's3://{self.output_bucket}/{prefix}/{base_name}_crf_{crf}.{ext}'
                else:
                    task.qp = qp
                    task.destination_url = f's3://{self.output_bucket}/{prefix}/{base_name}_qp_{qp}.{ext}'
                session.add(task)

            if task.status in (Status.SUCESS, Status.FAILED):
                logging.error(f'Task {task.pk} is finished')
                return

            task.status = Status.IN_PROGRESS
            session.commit()
            task_id = task.pk
            session.expunge(task)

        parsed_source = urlparse(task.source_url)
        presigned_url = self.s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': parsed_source.netloc, 'Key': parsed_source.path.lstrip('/')},
            ExpiresIn=3600 * 24,
        )

        parsed_destination = urlparse(task.destination_url)

        with NamedTemporaryFile(suffix='.mp4') as output_file:
            logging.info(f'Encoding file {task.source_url}')
            try:
                self.encode_video(presigned_url, output_file.name, task.crf, task.qp)
                self.s3_client.upload_file(
                    Filename=output_file.name,
                    Bucket=parsed_destination.netloc,
                    Key=parsed_destination.path.lstrip('/'),
                )
            except Exception as e:
                logging.exception('Failed processing task')
                task.status = Status.FAILED
                task.details = str(e)
            else:
                task.status = Status.SUCESS

        with self.session_maker.begin() as session:
            session.merge(task)
            session.commit()

        return {'task_id': task_id, 'status': task.status.value}


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
    )
    app.conf.broker_transport_options = {'is_secure': True}

    return app


load_dotenv()
configure_logging()
celery = configure_celery()
transcode_video_task = celery.register_task(TranscodeVideoTask)
