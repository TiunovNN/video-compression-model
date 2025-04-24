import logging
import shlex
import shutil
import subprocess
from functools import cached_property
from tempfile import NamedTemporaryFile
from typing import Optional
from uuid import uuid4

import boto3
from celery import Celery, Task as CeleryTask
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from database import Task, TaskStatus
from settings import Settings


class TranscodeVideoTask(CeleryTask):
    name = 'transcode_video'

    @cached_property
    def session_maker(self):
        engine = create_engine(self.app.conf.get('database_url'))
        return sessionmaker(engine, expire_on_commit=False)

    @cached_property
    def s3_client(self):
        s3_access_key_id = self.app.conf.get('s3_access_key_id')
        s3_secret_access_key = self.app.conf.get('s3_secret_access_key')
        s3_endpoint_url = self.app.conf.get('s3_endpoint_url')

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
            executable_name,
        ]

        for bin_ in possible_bin_dirs:
            if resolved_binary := shutil.which(bin_):
                return resolved_binary

        raise FileNotFoundError(f"{executable_name} not found in PATH")

    @property
    def s3_bucket(self):
        return self.app.conf.get('s3_bucket')

    def encode_video(
        self,
        input_path: str,
        output_path: str,
    ):
        """Encode a video using ffmpeg with the given parameters."""
        encode_params = self.encode_params(input_path)
        try:
            logging.info(f'Encoding video with {shlex.join(encode_params)}')

            # Build the ffmpeg command
            input_params = [
                '-seekable', '1',
                '-reconnect_delay_max', '300',
                '-multiple_requests', '1',
                '-reconnect_on_http_error', '429,5xx',
                '-reconnect_on_network_error', '1',
                '-i', input_path,
            ]

            global_params = [
                '-codec:a', 'copy',
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
            logging.error(f'Error encoding video: {e.stderr}')
            raise RuntimeError(str(e)) from e
        except Exception as e:
            logging.error(f'Unexpected error during encoding: {str(e)}')
            raise RuntimeError(f'Unknown error {e}') from e

    def run(self, task_id: int):
        with self.session_maker.begin() as session:
            task = session.query(Task).filter_by(
                id=task_id,
            ).first()

            if task.status in (TaskStatus.COMPLETED, TaskStatus.FAILED):
                logging.error(f'Task {task.id} is finished')
                return

            task.status = TaskStatus.PROCESSING
            session.commit()
            task_id = task.id
            session.expunge(task)
        output_key = f'encoded/{uuid4().hex}.mp4'

        presigned_url = self.s3_client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': self.s3_bucket,
                'Key': task.source_file.lstrip('/'),
            },
            ExpiresIn=3600 * 24,
        )

        with NamedTemporaryFile(suffix='.mp4') as output_file:
            logging.info(f'Encoding file {task.source_file}')
            try:
                self.encode_video(presigned_url, output_file.name)
                self.s3_client.upload_file(
                    Filename=output_file.name,
                    Bucket=self.s3_bucket,
                    Key=output_key,
                )
            except Exception as e:
                logging.exception('Failed processing task')
                task.status = TaskStatus.FAILED
                task.error_message = str(e)
            else:
                task.status = TaskStatus.COMPLETED
                task.output_file = output_key

        with self.session_maker.begin() as session:
            session.merge(task)
            session.commit()

        return {'task_id': task_id, 'status': task.status.value}

    def encode_params(self, input_path) -> list[str]:
        """Stub for ML model"""
        return [
            '-c:v', 'libx265',
            '-preset', 'ultrafast',
            '-crf', '16',
        ]


def configure_celery(settings: Settings):
    app = Celery('encoder_tasks')

    app.conf.update(
        broker_url=settings.CELERY_BROKER_URL,
        result_backend=f'db+{settings.DATABASE_URL}',
        s3_endpoint_url=settings.S3_ENDPOINT_URL,
        s3_access_key_id=settings.AWS_ACCESS_KEY_ID,
        s3_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        s3_bucket=settings.S3_BUCKET,
        database_url=settings.DATABASE_URL,
        task_default_queue=settings.CELERY_QUEUE_NAME,
    )
    # Special for SQS
    app.conf.broker_transport_options = {'is_secure': True}

    return app
