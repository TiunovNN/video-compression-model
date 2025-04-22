import io
import random
from http import HTTPStatus
from unittest.mock import AsyncMock, MagicMock, call
from uuid import uuid4

import pytest
from anys import ANY_AWARE_DATETIME_STR, ANY_INT, AnyInstance, AnyMatch
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from database import Base, Task, TaskStatus
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


async def test_create_task(client, s3_client_mock, transcode_video_mock):
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
    assert s3_client_mock.s3.upload_fileobj.call_args == call(
        AnyInstance(io.IOBase),
        'test-bucket',
        response.json()['source_file']
    )
    assert transcode_video_mock.delay.call_args == call(
        response.json()['id'],
    )


@pytest.fixture()
async def generate_tasks(db_session):
    tasks = []
    for status in TaskStatus:
        task = await create_task(db_session, status)
        tasks.append(task)
    return tasks


async def create_task(db_session, status) -> Task:
    task = Task(
        status=TaskStatus.PENDING,
        source_file=f'source/{uuid4().hex}.mp4',
    )
    db_session.add(task)
    await db_session.commit()
    await db_session.refresh(task)
    if status != TaskStatus.PENDING:
        task.status = status
        if status == TaskStatus.FAILED:
            task.error_message = 'test error'
        elif status == TaskStatus.COMPLETED:
            task.output_file = f'encoded/{uuid4().hex}.mp4'
        await db_session.commit()
    return task


async def test_list_tasks(client, generate_tasks):
    response = await client.get('/tasks')
    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        'tasks': [
            {
                'id': ANY_INT,
                'status': 'pending',
                'created_at': ANY_AWARE_DATETIME_STR,
                'updated_at': ANY_AWARE_DATETIME_STR,
                'output_file': None,
                'source_file': AnyMatch('source/[0-9a-f]{32}.mp4'),
                'error_message': None,
            },
            {
                'id': ANY_INT,
                'status': 'processing',
                'created_at': ANY_AWARE_DATETIME_STR,
                'updated_at': ANY_AWARE_DATETIME_STR,
                'output_file': None,
                'source_file': AnyMatch('source/[0-9a-f]{32}.mp4'),
                'error_message': None,
            },
            {
                'id': ANY_INT,
                'status': 'completed',
                'created_at': ANY_AWARE_DATETIME_STR,
                'updated_at': ANY_AWARE_DATETIME_STR,
                'output_file': AnyMatch('encoded/[0-9a-f]{32}.mp4'),
                'source_file': AnyMatch('source/[0-9a-f]{32}.mp4'),
                'error_message': None,
            },
            {
                'id': ANY_INT,
                'status': 'failed',
                'created_at': ANY_AWARE_DATETIME_STR,
                'updated_at': ANY_AWARE_DATETIME_STR,
                'output_file': None,
                'source_file': AnyMatch('source/[0-9a-f]{32}.mp4'),
                'error_message': 'test error',
            },
        ]
    }


async def test_get_pending_task(client, db_session, s3_client_mock):
    pending_task = await create_task(db_session, TaskStatus.PENDING)
    response = await client.get(f'/tasks/{pending_task.id}')
    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        'id': pending_task.id,
        'status': 'pending',
        'created_at': ANY_AWARE_DATETIME_STR,
        'updated_at': ANY_AWARE_DATETIME_STR,
        'output_file': None,
        'source_file': pending_task.source_file,
        'error_message': None,
        'download_url': None,
    }


async def test_get_processing_task(client, db_session, s3_client_mock):
    processing_task = await create_task(db_session, TaskStatus.PROCESSING)
    response = await client.get(f'/tasks/{processing_task.id}')
    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        'id': processing_task.id,
        'status': 'processing',
        'created_at': ANY_AWARE_DATETIME_STR,
        'updated_at': ANY_AWARE_DATETIME_STR,
        'output_file': None,
        'source_file': processing_task.source_file,
        'error_message': None,
        'download_url': None,
    }


async def test_get_completed_task(client, db_session, s3_client_mock):
    s3_client_mock.s3.generate_presigned_url.return_value = 'https://test.s3.amazonaws.com/test.mp4?signed_url'
    completed_task = await create_task(db_session, TaskStatus.COMPLETED)
    response = await client.get(f'/tasks/{completed_task.id}')
    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        'id': completed_task.id,
        'status': 'completed',
        'created_at': ANY_AWARE_DATETIME_STR,
        'updated_at': ANY_AWARE_DATETIME_STR,
        'output_file': completed_task.output_file,
        'source_file': completed_task.source_file,
        'error_message': None,
        'download_url': 'https://test.s3.amazonaws.com/test.mp4?signed_url',
    }
    assert s3_client_mock.s3.generate_presigned_url.call_args == call(
        'get_object',
        Params={
            'Bucket': 'test-bucket',
            'Key': completed_task.output_file
        },
        ExpiresIn=100,
    )


async def test_get_failed_task(client, db_session, s3_client_mock):
    failed_task = await create_task(db_session, TaskStatus.FAILED)
    response = await client.get(f'/tasks/{failed_task.id}')
    assert response.status_code == HTTPStatus.OK, response.json()
    assert response.json() == {
        'id': failed_task.id,
        'status': 'failed',
        'created_at': ANY_AWARE_DATETIME_STR,
        'updated_at': ANY_AWARE_DATETIME_STR,
        'output_file': None,
        'source_file': failed_task.source_file,
        'error_message': 'test error',
        'download_url': None,
    }
