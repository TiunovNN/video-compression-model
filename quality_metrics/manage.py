import logging
import sys

import click
from dotenv import load_dotenv
from sqlalchemy import URL, create_engine
from sqlalchemy.orm import Session
from tqdm import tqdm

from models import EncoderTask, Status
from worker import quality_analyze_task


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
def generate_tasks(
    s3_access_key_id: str,
    s3_secret_access_key: str,
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
    engine = create_engine(url)
    with Session(engine) as session:
        cursor = session.query(EncoderTask).filter_by(
            status=Status.SUCESS,
        )
        for task in tqdm(cursor):
            task: EncoderTask
            quality_analyze_task.delay(task.source_url, task.destination_url)


if __name__ == '__main__':
    load_dotenv()
    configure_logging()
    cli = click.Group(commands=[generate_tasks])
    cli()
