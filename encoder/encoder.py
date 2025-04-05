import logging
import shutil
import subprocess
import sys
from tempfile import NamedTemporaryFile
from typing import Iterator, Optional
from urllib.parse import urlparse

import boto3
import click
from dotenv import load_dotenv
from sqlalchemy import URL, create_engine
from sqlalchemy.orm import sessionmaker

from models import Base, EncoderTask, Status
from worker import transcode_video_task


def configure_logging():
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setLevel(logging.INFO)
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s\n',
        handlers=[handler],
        force=True,
    )


def iter_over_bucket(client, bucket) -> Iterator[str]:
    args = {'Bucket': bucket, 'MaxKeys': 500}
    logging.info('list s3 objects')
    res = client.list_objects_v2(**args)
    while res['KeyCount'] > 0:
        logging.info(f'got {res["KeyCount"]} objects')
        for content in res['Contents']:
            yield content['Key']

        token_args = {}
        if next_token := res.get('NextContinuationToken'):
            token_args['ContinuationToken'] = next_token
            res = client.list_objects_v2(**args, **token_args)
        else:
            break


def find_local_executable_path(executable_name: str) -> Optional[str]:
    possible_bin_dirs = [
        f'./{executable_name}',
        # Executable can be in same dir when graph build in "UNION" with required binaries
        executable_name,
    ]

    for bin_ in possible_bin_dirs:
        if resolved_binary := shutil.which(bin_):
            return resolved_binary


def encode_video(
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
                'ffmpeg',
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


@click.command()
@click.option(
    '--s3-access-key-id',
    type=click.STRING,
    envvar='ENCODER_S3_ACCESS_KEY_ID',
    required=True
)
@click.option(
    '--s3-secret-access-key',
    type=click.STRING,
    envvar='ENCODER_S3_SECRET_ACCESS_KEY',
    required=True
)
@click.option(
    '--database', '-d',
    type=click.STRING,
    required=True,
)
@click.option(
    '--database-user',
    type=click.STRING,
    envvar='DB_USER',
    required=False,
)
@click.option(
    '--database-password',
    type=click.STRING,
    envvar='DB_PASSWORD',
    required=False,
)
@click.option(
    '--database-driver',
    type=click.STRING,
    default='sqlite'
)
def process_one(
    s3_access_key_id: str,
    s3_secret_access_key: str,
    database: str,
    database_user: str,
    database_password: str,
    database_driver: str,
):
    engine = create_engine(
        URL.create(
            drivername=database_driver,
            host=database,
            username=database_user,
            password=database_password,
        ).render_as_string(hide_password=False)
    )
    session_maker = sessionmaker(engine, expire_on_commit=False)
    with session_maker.begin() as session:
        task = session.query(EncoderTask).filter(
            EncoderTask.status == Status.ENQUEUED
        ).first()
        if not task:
            logging.info('No pending tasks found')
            return
        task.status = Status.IN_PROGRESS
        session.commit()
        session.expunge(task)

    s3_client = boto3.client(
        's3',
        endpoint_url='https://storage.yandexcloud.net/',
        aws_access_key_id=s3_access_key_id,
        aws_secret_access_key=s3_secret_access_key,
    )
    parsed_source = urlparse(task.source_url)
    presigned_url = s3_client.generate_presigned_url(
        'get_object',
        Params={'Bucket': parsed_source.netloc, 'Key': parsed_source.path.lstrip('/')},
        ExpiresIn=3600 * 24,
    )
    parsed_destination = urlparse(task.destination_url)
    with NamedTemporaryFile(suffix='.mp4') as output_file:
        logging.info(f'Encoding file {task.source_url}')
        try:
            encode_video(presigned_url, output_file.name, task.crf, task.qp)
            s3_client.upload_file(
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

    with session_maker.begin() as session:
        session.merge(task)
        session.commit()


@click.command()
@click.option(
    '--database', '-d',
    envvar='DATABASE_HOST',
    type=click.STRING,
    required=True,
)
@click.option(
    '--database-user',
    type=click.STRING,
    envvar='DATABASE_USER',
    required=False,
)
@click.option(
    '--database-password',
    type=click.STRING,
    envvar='DATABASE_PASSWORD',
    required=False,
)
@click.option(
    '--database-driver',
    envvar='DATABASE_DRIVER',
    type=click.STRING,
    default='sqlite',
)
@click.option(
    '--database-name',
    envvar='DATABASE_NAME',
    type=click.STRING,
)
@click.option(
    '--database-port',
    envvar='DATABASE_PORT',
    type=click.INT,
    default=5432,
)
def create_table(
    database: str,
    database_user: str,
    database_password: str,
    database_driver: str,
    database_name: str,
    database_port: int,
):
    url = URL.create(
        drivername=database_driver,
        host=database,
        username=database_user,
        password=database_password,
        database=database_name,
        port=database_port,
    ).render_as_string(hide_password=False)
    print(url)
    engine = create_engine(url)
    Base.metadata.create_all(engine)


@click.command()
@click.option(
    '--qp-min',
    type=click.INT,
    required=True,
)
@click.option(
    '--qp-max',
    type=click.INT,
    required=True,
)
@click.option(
    '--crf-min',
    type=click.INT,
    required=True,
)
@click.option(
    '--crf-max',
    type=click.INT,
    required=True,
)
@click.option(
    '--s3-access-key-id',
    type=click.STRING,
    envvar='S3_ACCESS_KEY_ID',
    required=True
)
@click.option(
    '--s3-secret-access-key',
    type=click.STRING,
    envvar='S3_SECRET_ACCESS_KEY',
    required=True
)
@click.option('--input-bucket', type=click.STRING, required=True)
def generate_tasks(
    s3_access_key_id: str,
    s3_secret_access_key: str,
    input_bucket: str,
    qp_min: int,
    qp_max: int,
    crf_min: int,
    crf_max: int,
):
    s3_client = boto3.client(
        's3',
        endpoint_url='https://storage.yandexcloud.net/',
        aws_access_key_id=s3_access_key_id,
        aws_secret_access_key=s3_secret_access_key,
    )
    for path in iter_over_bucket(s3_client, input_bucket):
        source_url = f's3://{input_bucket}/{path}'
        prefix, _, filename = path.rpartition('/')
        base_name, _, ext = path.rpartition('.')
        for qp in range(qp_min, qp_max + 1):
            transcode_video_task.delay(source_url=source_url, qp=qp)

        for crf in range(crf_min, crf_max + 1):
            transcode_video_task.delay(source_url=source_url, crf=crf)


if __name__ == '__main__':
    load_dotenv()
    configure_logging()
    cli = click.Group(commands=[create_table, generate_tasks, process_one])
    cli()
