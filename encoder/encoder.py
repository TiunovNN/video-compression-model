import logging
import sys

import boto3
import click
from dotenv import load_dotenv
from sqlalchemy import create_engine, URL


def configure_logging():
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setLevel(logging.INFO)
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s\n',
        handlers=[handler],
        force=True,
    )


@click.command()
@click.option('--path', type=click.STRING, required=True)
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
    required=True,
)
@click.option(
    '--database-password',
    type=click.STRING,
    envvar='DB_PASSWORD',
    required=True,
)
@click.option('--input-bucket', type=click.STRING, required=True)
@click.option('--output-bucket', type=click.STRING, required=True)
def process_one(
    path: str,
    s3_access_key_id: str,
    s3_secret_access_key: str,
    database: str,
    database_user: str,
    database_password: str,
    input_bucket: str,
    output_bucket: str,
):
    engine = create_engine(URL.create(
        'postgresql',
        host=database,
        username=database_user,
        password=database_password,
    ))
    s3_client = boto3.client(
        's3',
        endpoint_url='https://storage.yandexcloud.net/',
        aws_access_key_id=s3_access_key_id,
        aws_secret_access_key=s3_secret_access_key,
    )
    presigned_url = s3_client.generate_presigned_url(
        'get_object',
        Params={'Bucket': input_bucket, 'Key': path},
        ExpiresIn=3600 * 24,
    )
    logging.info(f'Encoding file {input_bucket}/{path}')
    # TODO: Encode file
    logging.info(f'Uploading {0} to {output_bucket}/{path}.csv')
    # s3_client.put_object(
    #     Bucket=output_bucket,
    #     Key=f'{path}.csv',
    #     Body=csv_data,
    #     Metadata={'Content-Type': 'text/csv'}
    # )


if __name__ == '__main__':
    load_dotenv()
    configure_logging()
    cli = click.Group(commands=[])
    cli()
