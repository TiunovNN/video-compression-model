from typing import Annotated

from celery import Celery
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from database import async_session
from s3_client import S3Client
from settings import Settings, get_settings
from tasks import TranscodeVideoTask, configure_celery

APISettings = Annotated[Settings, Depends(get_settings)]


async def get_db(settings: APISettings) -> AsyncSession:
    async with async_session(settings) as session:
        yield session


async def get_s3_client(setting: APISettings) -> S3Client:
    async with S3Client(setting) as client:
        yield client


def get_celery_app(setting: APISettings) -> Celery:
    celery = configure_celery(setting)
    return celery


APICelery = Annotated[Celery, Depends(get_celery_app)]


def get_transcode_video_task(celery_app: APICelery) -> TranscodeVideoTask:
    return celery_app.register_task(TranscodeVideoTask)


DBSession = Annotated[AsyncSession, Depends(get_db)]
S3ClientAPI = Annotated[S3Client, Depends(get_s3_client)]
TranscodeVideoTaskAPI = Annotated[TranscodeVideoTask, Depends(get_transcode_video_task)]
