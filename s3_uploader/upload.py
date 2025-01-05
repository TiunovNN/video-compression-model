import logging
import sqlite3
import sys
from concurrent.futures.thread import ThreadPoolExecutor
from tempfile import NamedTemporaryFile

import boto3
import click
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from dotenv import load_dotenv
from requests import Session
from requests.adapters import HTTPAdapter, Retry


class Copier:
    default_s3_client_config = Config(
        # Standard retry mode
        # https://boto3.amazonaws.com/v1/documentation/api/1.16.56/guide/retries.html#standard-retry-mode
        retries={
            'mode': 'standard',
            'max_attempts': 192,
        }
    )

    def __init__(
        self,
        s3_access_key_id,
        s3_secret_access_key,
        bucket,
        concurrency
    ):
        self.http_client = self._create_session(concurrency)
        self.s3_client = boto3.client(
            's3',
            endpoint_url='https://storage.yandexcloud.net/',
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key,
            config=self.default_s3_client_config
        )
        self.bucket = bucket
        self.logger = logging.getLogger('Copier')
        self.transfer_config = TransferConfig(
            multipart_threshold=5 * 2 ** 30,
            max_concurrency=concurrency * 10,
            multipart_chunksize=8 * 2 ** 20,
            use_threads=True,
        )

    @staticmethod
    def _create_session(concurrency):
        retry_strategy = Retry(
            total=15,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=['HEAD', 'GET', 'OPTIONS'],
            backoff_factor=0.1
        )
        session = Session()
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_maxsize=concurrency,
            pool_connections=concurrency,
            pool_block=True,
        )
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session

    def copy_file_to_s3(self, src_url, dst_path):
        self.logger.info(f'Downloading from {src_url} to {dst_path}')
        try:
            self.s3_client.head_object(
                Bucket=self.bucket,
                Key=dst_path,
            )
            return
        except self.s3_client.exceptions.NoSuchKey:
            pass

        with self.http_client.get(src_url, stream=True) as response:
            response.raise_for_status()
            tmp_context = NamedTemporaryFile(
                buffering=10 * 2 ** 20,
                suffix='.mkv',
                prefix=dst_path.rpartition('/')[-1]
            )
            with tmp_context as tempfile:
                for chunk in response.iter_content(8192):
                    tempfile.write(chunk)

                tempfile.flush()
                tempfile.seek(0)
                self.s3_client.upload_fileobj(
                    tempfile,
                    Bucket=self.bucket,
                    Key=dst_path,
                    ExtraArgs={"ContentType": response.headers['Content-Type']},
                    Config=self.transfer_config
                )
        self.logger.info(f'Finished uploading {src_url}')

    def process_item(self, item: dict[str, str]) -> tuple[str, bool]:
        try:
            self.copy_file_to_s3(item['src'], item['dst'])
        except Exception:
            self.logger.exception('Failed to copy file to S3 %s', item['src'])
            return item['src'], False

        return item['src'], True


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
@click.option('--s3-access-key-id', type=click.STRING, required=True)
@click.option('--s3-secret-access-key', type=click.STRING, required=True)
@click.option('--bucket', type=click.STRING, required=True)
@click.option('--db-name', type=click.STRING, default='upload_progress.db')
@click.option('--concurrency', type=click.INT, default=1)
def main(
    s3_access_key_id: str,
    s3_secret_access_key: str,
    bucket: str,
    db_name: str,
    concurrency: int,
):
    configure_logging()
    con = sqlite3.connect(db_name)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    res = cur.execute("SELECT * FROM upload_progress WHERE success IS NULL")
    copier = Copier(
        s3_access_key_id=s3_access_key_id,
        s3_secret_access_key=s3_secret_access_key,
        bucket=bucket,
    )
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        for result in executor.map(copier.process_item, res.fetchall()):
            src_url, success = result
            cur.execute("UPDATE upload_progress SET success = ? WHERE src = ?", (success, src_url))
            con.commit()


if __name__ == '__main__':
    load_dotenv()
    main(auto_envvar_prefix='UPLOADER')
