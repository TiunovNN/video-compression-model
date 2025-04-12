import logging
import sys
from typing import Iterator

import boto3
import click
from dotenv import load_dotenv
from sqlalchemy import URL, create_engine

from models import Base
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
