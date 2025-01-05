import csv
import io

import boto3
import click
from dotenv import load_dotenv

from decoder import Decoder
from features import SICalculator, TICalculator


@click.command()
@click.option('--path', type=click.STRING, required=True)
@click.option('--s3-access-key-id', type=click.STRING, required=True)
@click.option('--s3-secret-access-key', type=click.STRING, required=True)
@click.option('--input-bucket', type=click.STRING, required=True)
@click.option('--output-bucket', type=click.STRING, required=True)
def main(
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
    feature_calculators = [
        SICalculator(),
        TICalculator(),
    ]
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

    writer = csv.DictWriter(buffer, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL, quotechar='|')
    writer.writeheader()
    with Decoder(presigned_url) as decoder:
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
            frame_data = frame.to_ndarray()
            for calculator in feature_calculators:
                item[calculator.name()] = calculator.feed_frame(frame_data)
            writer.writerow(item)

    buffer.seek(0)
    s3_client.put_object(
        Bucket=output_bucket,
        Key=f'{path}.csv',
        Body=buffer.getvalue().encode(),
        Metadata={'Content-Type': 'text/csv'}
    )


if __name__ == '__main__':
    load_dotenv()
    main(auto_envvar_prefix='CALCULATOR')
