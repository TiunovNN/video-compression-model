import logging
import subprocess
import sys
from typing import Iterator

import boto3
import click
from dotenv import load_dotenv
from sqlalchemy import URL, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.sync import update

from models import Base, EncoderTask, Status


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


def download_file(url, local_path):
    """Download a file from a URL to a local path."""
    try:
        logging.info(f"Downloading file from {url} to {local_path}")
        with requests.get(url, stream=True) as response:
            response.raise_for_status()

            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

        return True
    except Exception as e:
        logging.error(f"Error downloading file: {str(e)}")
        return False


def upload_file(local_path, destination_url):
    """Upload a file to a destination URL."""
    try:
        logging.info(f"Uploading file from {local_path} to {destination_url}")
        with open(local_path, 'rb') as f:
            files = {'file': f}
            with requests.post(destination_url, files=files) as response:
                response.raise_for_status()

        return True
    except Exception as e:
        logging.error(f"Error uploading file: {str(e)}")
        return False


def encode_video(input_path, output_path, crf, qp):
    """Encode a video using ffmpeg with the given parameters."""
    try:
        logging.info(f"Encoding video with CRF={crf}, QP={qp}")

        # Build the ffmpeg command
        cmd = [
            'ffmpeg',
            '-i', input_path,
            '-c:v', 'libx264',
            '-crf', str(crf),
            '-qp', str(qp),
            '-c:a', 'aac',
            '-y',
            output_path
        ]

        # Run the command
        process = subprocess.run(cmd, check=True, capture_output=True, text=True)

        return True, process.stdout
    except subprocess.CalledProcessError as e:
        logging.error(f"Error encoding video: {e.stderr}")
        return False, e.stderr
    except Exception as e:
        logging.error(f"Unexpected error during encoding: {str(e)}")
        return False, str(e)


def process_task(task: EncoderTask):
    """Process a single encoding task."""
    logging.info(f"Processing task {task.pk}: {task.source_url} -> {task.destination_url}")

    # Update task status to IN_PROGRESS
    with get_db_session() as session:
        task.status = Status.IN_PROGRESS
        task.save()

    try:
        # Create temporary file paths
        input_path = f"temp_input_{task.pk}.mp4"
        output_path = f"temp_output_{task.pk}.mp4"

        # Download the source file
        if not download_file(task.source_url, input_path):
            raise Exception("Failed to download source file")

        # Encode the video
        success, encoding_details = encode_video(input_path, output_path, task.crf, task.qp)
        if not success:
            raise Exception(f"Failed to encode video: {encoding_details}")

        # Upload the result
        if not upload_file(output_path, task.destination_url):
            raise Exception("Failed to upload encoded file")

        # Update task status to SUCCESS
        with get_db_session() as session:
            session.execute(
                update(EncoderTask)
                .where(EncoderTask.pk == task.pk)
                .values(status=Status.SUCESS, details="Task completed successfully")
            )

        logging.info(f"Task {task.pk} processed successfully")

    except Exception as e:
        error_details = f"Error: {str(e)}\n{traceback.format_exc()}"
        logging.error(f"Failed to process task {task.pk}: {error_details}")

        # Update task status to FAILED
        with get_db_session() as session:
            session.execute(
                update(EncoderTask)
                .where(EncoderTask.pk == task.pk)
                .values(status=Status.FAILED, details=error_details)
            )

    finally:
        # Clean up temporary files
        for path in [input_path, output_path]:
            if os.path.exists(path):
                try:
                    os.remove(path)
                except Exception as e:
                    logging.warning(f"Failed to remove temporary file {path}: {str(e)}")


def fetch_pending_task():
    """Fetch a single pending task from the database."""
    with get_db_session() as session:
        task = session.query(EncoderTask).filter(
            EncoderTask.status == Status.ENQUEUED
        ).first()

        # Return a detached copy of the task to avoid session issues
        if task:
            return EncoderTask(
                pk=task.pk,
                source_url=task.source_url,
                destination_url=task.destination_url,
                crf=task.crf,
                qp=task.qp,
                status=task.status,
                details=task.details
            )
        return


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
@click.option('--input-bucket', type=click.STRING, required=True)
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
    path: str,
    s3_access_key_id: str,
    s3_secret_access_key: str,
    database: str,
    database_user: str,
    database_password: str,
    database_driver: str,
    input_bucket: str,
    output_bucket: str,
):
    engine = create_engine(
        URL.create(
            drivername=database_driver,
            host=database,
            username=database_user,
            password=database_password,
        ).render_as_string(hide_password=False)
    )
    session = sessionmaker(engine)

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


@click.command()
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
def create_table(
    database: str,
    database_user: str,
    database_password: str,
    database_driver: str,
):
    url = URL.create(
        drivername=database_driver,
        host=database,
        username=database_user,
        password=database_password,
    ).render_as_string(hide_password=False)
    engine = create_engine(url)
    Base.metadata.create_all(engine)


@click.command()
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
    envvar='ENCODER_S3_ACCESS_KEY_ID',
    required=True
)
@click.option(
    '--s3-secret-access-key',
    type=click.STRING,
    envvar='ENCODER_S3_SECRET_ACCESS_KEY',
    required=True
)
@click.option('--input-bucket', type=click.STRING, required=True)
@click.option('--output-bucket', type=click.STRING, required=True)
def generate_tasks(
    s3_access_key_id: str,
    s3_secret_access_key: str,
    input_bucket: str,
    output_bucket: str,
    database: str,
    database_user: str,
    database_password: str,
    database_driver: str,
    qp_min: int,
    qp_max: int,
    crf_min: int,
    crf_max: int,
):
    url = URL.create(
        drivername=database_driver,
        host=database,
        username=database_user,
        password=database_password,
    ).render_as_string(hide_password=False)
    engine = create_engine(url)
    session_maker = sessionmaker(engine)
    s3_client = boto3.client(
        's3',
        endpoint_url='https://storage.yandexcloud.net/',
        aws_access_key_id=s3_access_key_id,
        aws_secret_access_key=s3_secret_access_key,
    )
    with session_maker.begin() as session:
        for path in iter_over_bucket(s3_client, input_bucket):
            source_url = f's3://{input_bucket}/{path}'
            prefix, _, filename = path.rpartition('/')
            base_name, _, ext = path.rpartition('.')
            for qp in range(qp_min, qp_max + 1):
                if not session.query(EncoderTask).filter_by(source_url=source_url, qp=qp).first():
                    session.add(
                        EncoderTask(
                            source_url=source_url,
                            destination_url=f's3://{output_bucket}/{prefix}/{base_name}_qp_{qp}.{ext}',
                            qp=qp,
                        )
                    )
            for crf in range(crf_min, crf_max + 1):
                if not session.query(EncoderTask).filter_by(source_url=source_url, crf=crf).first():
                    session.add(
                        EncoderTask(
                            source_url=source_url,
                            destination_url=f's3://{output_bucket}/{prefix}/{base_name}_crf_{crf}.{ext}',
                            crf=crf,
                        )
                    )
        session.commit()


if __name__ == '__main__':
    load_dotenv()
    configure_logging()
    cli = click.Group(commands=[create_table, generate_tasks])
    cli()
