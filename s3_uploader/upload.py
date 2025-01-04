import io
import logging
import sqlite3
import sys
from concurrent.futures.thread import ThreadPoolExecutor

import boto3
import click
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from dotenv import load_dotenv
from requests import Response, Session
from requests.adapters import HTTPAdapter, Retry
from s3transfer.upload import (
    UploadFilenameInputManager,
    UploadNonSeekableInputManager,
    UploadSeekableInputManager,
    UploadSubmissionTask,
)


class FileLikeResponse(io.RawIOBase):
    def __init__(self, resp: Response):
        self.resp = resp
        self.buffer = bytearray()
        self.iterator = iter(resp.iter_content(chunk_size=10 * 2 ** 10))
        self._tell = 0

    def read(self, __size=-1):
        logging.info(f'read {__size}')
        while len(self.buffer) < __size and self.iterator is not None:
            try:
                data = next(self.iterator)
            except StopIteration:
                self.iterator = None
            else:
                self.buffer.extend(data)

        result = self.buffer[:__size]
        self.buffer = self.buffer[__size:]
        self._tell += len(result)
        return result

    def readable(self):
        return True

    def size(self):
        return self.resp.headers['Content-Length']

    def tell(self):
        return self._tell


class UploadFileLikeResponseInputManager(UploadSeekableInputManager):
    @classmethod
    def is_compatible(cls, upload_source):
        logging.info('Is compatible')
        return isinstance(upload_source, FileLikeResponse)

    def provide_transfer_size(self, transfer_future):
        fileobj: FileLikeResponse = transfer_future.meta.call_args.fileobj
        # To determine size, first determine the starting position
        # Seek to the end and then find the difference in the length
        # between the end and start positions.
        logging.info(fileobj.resp.headers)
        transfer_future.meta.provide_transfer_size(
            fileobj.size()
        )


def _get_upload_input_manager_cls(self, transfer_future):
    """Retrieves a class for managing input for an upload based on file type

    :type transfer_future: s3transfer.futures.TransferFuture
    :param transfer_future: The transfer future for the request

    :rtype: class of UploadInputManager
    :returns: The appropriate class to use for managing a specific type of
        input for uploads.
    """
    upload_manager_resolver_chain = [
        UploadFileLikeResponseInputManager,
        UploadFilenameInputManager,
        UploadSeekableInputManager,
        UploadNonSeekableInputManager,
    ]

    fileobj = transfer_future.meta.call_args.fileobj
    for upload_manager_cls in upload_manager_resolver_chain:
        if upload_manager_cls.is_compatible(fileobj):
            return upload_manager_cls
    raise RuntimeError(
        f'Input {fileobj} of type: {type(fileobj)} is not supported.'
    )


# dirty hack
UploadSubmissionTask._get_upload_input_manager_cls = _get_upload_input_manager_cls


class Copier:
    default_s3_client_config = Config(
        # Standard retry mode
        # https://boto3.amazonaws.com/v1/documentation/api/1.16.56/guide/retries.html#standard-retry-mode
        retries={
            'mode': 'standard',
            'max_attempts': 192,
        }
    )

    def __init__(self, s3_access_key_id, s3_secret_access_key, bucket):
        self.http_client = self._create_session(192)
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
            max_concurrency=2,
            multipart_chunksize=8 * 2 ** 20,
            use_threads=True,
        )

    @staticmethod
    def _create_session(retries):
        retry_strategy = Retry(
            total=retries,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=['HEAD', 'GET', 'OPTIONS'],
            backoff_factor=0.1
        )
        session = Session()
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_maxsize=256,
            pool_connections=256,
            pool_block=True,
        )
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session

    def copy_file_to_s3(self, src_url, dst_path):
        self.logger.info(f'Downloading from {src_url} to {dst_path}')
        with self.http_client.get(src_url) as response:
            response.raise_for_status()
            # with SpooledTemporaryFile(buffering=10*2**20) as f:
            #     for chunk in response.iter_content(chunk_size=1024):
            #         f.write(chunk)
            self.s3_client.upload_fileobj(
                FileLikeResponse(response),
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
def main(
    s3_access_key_id: str,
    s3_secret_access_key: str,
    bucket: str,
    db_name: str,
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
    with ThreadPoolExecutor(max_workers=20) as executor:
        for result in executor.map(copier.process_item, res.fetchall()):
            src_url, success = result
            cur.execute("UPDATE upload_progress SET success = ? WHERE src = ?", (success, src_url))
            con.commit()


if __name__ == '__main__':
    load_dotenv()
    main(auto_envvar_prefix='UPLOADER')
