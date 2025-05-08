from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix='')
    DATABASE_URL: str = 'postgresql://postgres:postgres@localhost:5432/video_encoding'

    # S3 configuration
    AWS_ACCESS_KEY_ID: str = ''
    AWS_SECRET_ACCESS_KEY: str = ''
    S3_ENDPOINT_URL: str = ''
    S3_BUCKET: str = ''

    # Presigned URL expiration (in seconds)
    PRESIGNED_URL_EXPIRATION: int = 3600

    CELERY_BROKER_URL: str = 'redis://localhost:6379/0'
    CELERY_QUEUE_NAME: str = 'api_transcoding'
    REGRESSOR_PATH: str = 'model.cbm'


def get_settings() -> Settings:
    return Settings()
