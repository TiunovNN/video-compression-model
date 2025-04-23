import io
import os
import uuid
from typing import Optional, Self

from aioboto3 import Session
from botocore.config import Config

from settings import Settings


class S3Exception(RuntimeError):
    pass


class FailedUploadS3(S3Exception):
    def __init__(self, filename: str, reason: Exception):
        super().__init__(f"Failed to upload file to S3: {filename} - {reason}")
        self.filename = filename
        self.reason = reason


class S3Client:
    def __init__(self, settings: Settings):
        self.session = Session()
        self.s3_client_context = self.session.client(
            's3',
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            endpoint_url=settings.S3_ENDPOINT_URL,
            config=Config(signature_version="s3v4")
        )
        self.s3 = None
        self.bucket_name = settings.S3_BUCKET
        self.presigned_url_expiration = settings.PRESIGNED_URL_EXPIRATION

    async def __aenter__(self) -> Self:
        if self.s3 is not None:
            raise RuntimeError('S3Client already initialized')

        self.s3 = await self.s3_client_context.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.s3 is None:
            raise RuntimeError('S3Client is not initialized')

        s3 = self.s3
        self.s3 = None
        return await s3.__aexit__(exc_type, exc_val, exc_tb)

    async def upload_file(
        self,
        file: io.FileIO,
        object_name: str,
        content_type: Optional[str] = None
    ) -> str:
        extra_data = {}
        if content_type:
            extra_data['ContentType'] = content_type
        try:
            await self.s3.upload_fileobj(
                file,
                self.bucket_name,
                object_name,
                ExtraArgs=extra_data,
            )
        except Exception as e:
            raise FailedUploadS3(object_name, e)
        return object_name

    async def generate_presigned_url(self, object_name):
        url = await self.s3.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': self.bucket_name,
                'Key': object_name
            },
            ExpiresIn=self.presigned_url_expiration
        )
        return url

    @staticmethod
    def generate_unique_filename(original_filename):
        extension = os.path.splitext(original_filename)[-1]
        return f"{uuid.uuid4().hex}{extension}"
