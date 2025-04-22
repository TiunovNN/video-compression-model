import random
from http import HTTPStatus
from unittest.mock import AsyncMock, MagicMock

import pytest
from anys import ANY_AWARE_DATETIME_STR, ANY_INT, AnyMatch
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from database import Base
from deps import get_db, get_s3_client, get_transcode_video_task
from main import app
from s3_client import S3Client

pytestmark = pytest.mark.anyio


@pytest.fixture()
async def db_session() -> AsyncSession:
    engine = create_async_engine('sqlite+aiosqlite://', echo=True)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
        await connection.run_sync(Base.metadata.create_all)
        TestingSessionLocal = sessionmaker(
            expire_on_commit=False,
            class_=AsyncSession,
            bind=engine,
        )
        async with TestingSessionLocal(bind=connection) as session:
            yield session
            await session.flush()
            await session.rollback()


@pytest.fixture
def anyio_backend():
    return 'asyncio'


@pytest.fixture
def s3_client_mock():
    settings = MagicMock()
    settings.S3_ENDPOINT_URL = 'http://localhost:9000'
    settings.S3_BUCKET = 'test-bucket'
    settings.PRESIGNED_URL_EXPIRATION = 100
    client = S3Client(settings)
    client.s3 = AsyncMock()
    return client


@pytest.fixture
def transcode_video_mock():
    return MagicMock()


@pytest.fixture
async def client(db_session, s3_client_mock, transcode_video_mock):
    async def _override_get_db():
        yield db_session

    async def _override_get_s3_client():
        yield s3_client_mock

    async def _override_get_transcode_video_task():
        yield transcode_video_mock

    app.dependency_overrides[get_s3_client] = _override_get_s3_client
    app.dependency_overrides[get_db] = _override_get_db
    app.dependency_overrides[get_transcode_video_task] = _override_get_transcode_video_task
    async with AsyncClient(transport=ASGITransport(app=app), base_url='http://test') as client:
        yield client


async def test_create_task(client):
    mp4_data = random.randbytes(100)
    response = await client.post(
        '/tasks',
        files={
            'file': ('video.mp4', mp4_data, 'video/mp4')
        },
    )
    assert response.status_code == HTTPStatus.CREATED, response.json()
    assert response.json() == {
        'id': ANY_INT,
        'status': 'pending',
        'created_at': ANY_AWARE_DATETIME_STR,
        'updated_at': ANY_AWARE_DATETIME_STR,
        'output_file': None,
        'source_file': AnyMatch('source/[0-9a-f]{32}.mp4'),
        'error_message': None,
    }
