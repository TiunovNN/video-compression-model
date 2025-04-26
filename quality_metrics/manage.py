import logging
import sys
from concurrent.futures.thread import ThreadPoolExecutor

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
def generate_tasks(
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
        with ThreadPoolExecutor() as pool:
            total = session.query(EncoderTask).filter_by(status=Status.SUCESS).count()
            cursor = session.query(EncoderTask).filter_by(
                status=Status.SUCESS,
            )
            futures = pool.map(
                lambda task: quality_analyze_task.delay(task.source_url, task.destination_url),
                cursor
            )
            for _ in tqdm(futures, total=total):
                pass


if __name__ == '__main__':
    load_dotenv()
    configure_logging()
    cli = click.Group(commands=[generate_tasks])
    cli()
