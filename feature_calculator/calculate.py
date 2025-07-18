import csv
import io
import logging
import sys
from collections import defaultdict
from concurrent.futures import (
    FIRST_COMPLETED,
    Future,
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    as_completed,
    wait,
)
from typing import Iterator

import boto3
import click
import numpy as np
from botocore.exceptions import ClientError
from dotenv import load_dotenv

from decoder import Decoder
from extractors import (
    CIExtractor,
    Extractor,
    FHV13Extractor,
    FSI13Extractor,
    GLCMExtractor,
    GLCMPropertyExtractor,
    SIExtractor,
    TICalculator,
    UExtractor,
    VExtractor,
    YExtractor,
)
from features import (
    FHV13Calculator,
    FeatureCalculator,
    MeanCalculator,
    STDCalculator,
)

Processor = Extractor | FeatureCalculator
ProcessorResult = np.ndarray | float | None


def configure_logging():
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setLevel(logging.INFO)
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s\n',
        handlers=[handler],
        force=True,
    )


def run_processor(processor: Processor, future: Future[np.ndarray]) -> tuple[Processor, ProcessorResult]:
    if isinstance(processor, Extractor):
        return processor, processor.extract(future.result())

    return processor, processor.feed_frame(future.result())


def analyze_file(presigned_url) -> bytes:
    extractors = [
        YExtractor(),
        UExtractor(),
        VExtractor(),
        SIExtractor(),
        TICalculator(),
        GLCMExtractor(),
        GLCMPropertyExtractor('correlation'),
        GLCMPropertyExtractor('contrast'),
        GLCMPropertyExtractor('energy'),
        GLCMPropertyExtractor('homogeneity'),
        CIExtractor('U'),
        CIExtractor('V', 1.5),
        FSI13Extractor(),
        FHV13Extractor(),
    ]
    feature_calculators = [
        STDCalculator('Y', 'CTI_std'),
        STDCalculator('SI'),
        STDCalculator('TI'),
        MeanCalculator('Y', 'CTI_mean'),
        MeanCalculator('SI'),
        MeanCalculator('TI'),
        MeanCalculator('GLCM_correlation'),
        MeanCalculator('GLCM_contrast'),
        MeanCalculator('GLCM_energy'),
        MeanCalculator('GLCM_homogeneity'),
        STDCalculator('GLCM_correlation'),
        STDCalculator('GLCM_contrast'),
        STDCalculator('GLCM_energy'),
        STDCalculator('GLCM_homogeneity'),
        MeanCalculator('FSI13'),
        MeanCalculator('CI_U'),
        MeanCalculator('CI_V'),
        FHV13Calculator(),
    ]
    processors = extractors + feature_calculators
    buffer = io.StringIO()
    fieldnames = [
        'width',
        'height',
        'format',
        'key_frame',
        'time',
        'pts',
        'dts',
    ]
    for calculator in feature_calculators:
        fieldnames.append(calculator.name())

    writer = csv.DictWriter(buffer, fieldnames=fieldnames, quoting=csv.QUOTE_STRINGS, delimiter='|')
    writer.writeheader()
    with Decoder(presigned_url) as decoder:
        with ThreadPoolExecutor() as pool:
            for frame in decoder:
                item = {
                    'width': frame.width,
                    'height': frame.height,
                    'format': frame.format.name,
                    'key_frame': int(frame.key_frame),
                    'time': frame.time,
                    'pts': frame.pts,
                    'dts': frame.dts,
                }
                extractors_results = defaultdict(Future)
                frame_data = frame.to_ndarray()
                extractors_results[None].set_result(frame_data)
                futures = []
                for processor in processors:
                    matrix_future = extractors_results[processor.depends_on()]
                    futures.append(pool.submit(run_processor, processor, matrix_future))

                for future in as_completed(futures):
                    calc, result = future.result()
                    if isinstance(calc, Extractor):
                        extractors_results[calc.name()].set_result(result)
                    else:
                        item[calc.name()] = result
                writer.writerow(item)
    return buffer.getvalue().encode()


class AnalyzeAndUploader:
    def __init__(self, s3_access_key_id, s3_secret_access_key, input_bucket, output_bucket):
        self.s3_access_key_id = s3_access_key_id
        self.s3_secret_access_key = s3_secret_access_key
        self.input_bucket = input_bucket
        self.output_bucket = output_bucket

    def __call__(self, path):
        s3_client = boto3.client(
            's3',
            endpoint_url='https://storage.yandexcloud.net/',
            aws_access_key_id=self.s3_access_key_id,
            aws_secret_access_key=self.s3_secret_access_key,
        )
        presigned_url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': self.input_bucket, 'Key': path},
            ExpiresIn=3600 * 24,
        )
        logging.info(f'Analyzing file {self.input_bucket}/{path}')
        try:
            csv_data = analyze_file(presigned_url)
        except Exception as e:
            logging.exception(f'Error while analyzing file {self.input_bucket}/{path}: {e}')
            return

        logging.info(f'Uploading {len(csv_data)} to {self.output_bucket}/{path}.csv')
        s3_client.put_object(
            Bucket=self.output_bucket,
            Key=f'{path}.csv',
            Body=csv_data,
            Metadata={'Content-Type': 'text/csv'}
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
@click.option('--path', type=click.STRING, required=True)
@click.option(
    '--s3-access-key-id',
    type=click.STRING,
    envvar='CALCULATOR_S3_ACCESS_KEY_ID',
    required=True
)
@click.option(
    '--s3-secret-access-key',
    type=click.STRING,
    envvar='CALCULATOR_S3_SECRET_ACCESS_KEY',
    required=True
)
@click.option('--input-bucket', type=click.STRING, required=True)
@click.option('--output-bucket', type=click.STRING, required=True)
def process_one(
    path: str,
    s3_access_key_id: str,
    s3_secret_access_key: str,
    input_bucket: str,
    output_bucket: str,
):
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
    logging.info(f'Analyzing file {input_bucket}/{path}')
    csv_data = analyze_file(presigned_url)
    logging.info(f'Uploading {len(csv_data)} to {output_bucket}/{path}.csv')
    s3_client.put_object(
        Bucket=output_bucket,
        Key=f'{path}.csv',
        Body=csv_data,
        Metadata={'Content-Type': 'text/csv'}
    )


@click.command()
@click.option(
    '--s3-access-key-id',
    type=click.STRING,
    envvar='CALCULATOR_S3_ACCESS_KEY_ID',
    required=True
)
@click.option(
    '--s3-secret-access-key',
    type=click.STRING,
    envvar='CALCULATOR_S3_SECRET_ACCESS_KEY',
    required=True
)
@click.option('--input-bucket', type=click.STRING, required=True)
@click.option('--output-bucket', type=click.STRING, required=True)
@click.option('--concurrency', type=click.INT, default=1)
@click.option('--rewrite', is_flag=True)
def process_bucket(
    s3_access_key_id: str,
    s3_secret_access_key: str,
    input_bucket: str,
    output_bucket: str,
    concurrency: int,
    rewrite: bool,
):
    s3_client = boto3.client(
        's3',
        endpoint_url='https://storage.yandexcloud.net/',
        aws_access_key_id=s3_access_key_id,
        aws_secret_access_key=s3_secret_access_key,
    )
    logging.info(f'Collecting paths from {input_bucket}')
    paths = []
    for path in iter_over_bucket(s3_client, input_bucket):
        if rewrite:
            paths.append(path)
            continue

        try:
            s3_client.head_object(
                Bucket=output_bucket,
                Key=f'{path}.csv',
            )
            continue
        except ClientError:
            pass
        paths.append(path)

    logging.info(f'Collected {len(paths)} paths')

    in_queue_futures = set()
    done = set()
    analyzer_and_uploader = AnalyzeAndUploader(
        s3_access_key_id,
        s3_secret_access_key,
        input_bucket,
        output_bucket,
    )
    with ProcessPoolExecutor(max_workers=concurrency, initializer=configure_logging) as pool:
        try:
            for path in paths:
                future = pool.submit(analyzer_and_uploader, path)
                in_queue_futures.add(future)
                if len(in_queue_futures) >= concurrency * 3:
                    for future in done:
                        future.result()
                    done, in_queue_futures = wait(in_queue_futures, return_when=FIRST_COMPLETED)

            for future in as_completed(done):
                future.result()
        except KeyboardInterrupt:
            pool.shutdown(cancel_futures=True)


if __name__ == '__main__':
    load_dotenv()
    configure_logging()
    cli = click.Group(commands=[process_one, process_bucket])
    cli()
