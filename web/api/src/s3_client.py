import os
import uuid

from aioboto3 import Session

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
        self.s3 = self.session.client(
            's3',
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            endpoint_url=settings.S3_ENDPOINT_URL,
        )
        self.bucket_name = settings.S3_BUCKET
        self.presigned_url_expiration = settings.PRESIGNED_URL_EXPIRATION

    async def upload_file(self, file_path: str, object_name: str) -> str:
        try:
            await self.s3.upload_file(file_path, self.bucket_name, object_name)
        except Exception as e:
            raise FailedUploadS3(file_path, e)
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
