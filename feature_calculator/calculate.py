import boto3
import click
from dotenv import load_dotenv

from decoder import Decoder


@click.command()
@click.option('--path', type=click.STRING, required=True)
@click.option('--s3-access-key-id', type=click.STRING, required=True)
@click.option('--s3-secret-access-key', type=click.STRING, required=True)
@click.option('--bucket', type=click.STRING, required=True)
def main(
    path: str,
    s3_access_key_id: str,
    s3_secret_access_key: str,
    bucket: str,
):
    s3_client = boto3.client(
        's3',
        endpoint_url='https://storage.yandexcloud.net/',
        aws_access_key_id=s3_access_key_id,
        aws_secret_access_key=s3_secret_access_key,
    )
    presigned_url = s3_client.generate_presigned_url(
        'get_object',
        Params={'Bucket': bucket, 'Key': path},
        ExpiresIn=3600 * 24,
    )
    with Decoder(presigned_url) as decoder:
        for frame in decoder:
            print(frame.width, frame.height, frame.key_frame, frame.time)


if __name__ == '__main__':
    load_dotenv()
    main(auto_envvar_prefix='CALCULATOR')
