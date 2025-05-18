import asyncio
import logging
from contextlib import asynccontextmanager
from http import HTTPStatus
from typing import Annotated

import magic
from fastapi import FastAPI, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import desc, select

import schemas
from database import Base, Task, TaskStatus, async_engine
from deps import (
    DBSession,
    FeatureCalculatorTaskAPI,
    S3ClientAPI,
    TranscodeVideoTaskAPI,
)
from s3_client import S3Exception
from schemas import TaskResponse
from settings import get_settings


@asynccontextmanager
async def on_startup(app: FastAPI):
    settings = get_settings()
    engine = async_engine(settings)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    yield


app = FastAPI(title="Video Encoding Service", lifespan=on_startup)
app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)


@app.post('/tasks', status_code=HTTPStatus.CREATED)
async def create_encoding_task(
    db: DBSession,
    file: UploadFile,
    s3_client: S3ClientAPI,
    transcode_video_task: TranscodeVideoTaskAPI,
    feature_calculator_task: FeatureCalculatorTaskAPI,
) -> schemas.TaskResponse:
    """
    Upload a video file and create an encoding task
    """
    # Check if file is a video
    file.file.seek(0)
    content_type = magic.from_buffer(file.file.read(1024), mime=True)
    logging.info(f'{content_type=}')
    file.file.seek(0)
    is_video = content_type.startswith('video/')

    if not is_video:
        raise HTTPException(status_code=400, detail='File must be a video')

    # Upload to S3
    s3_object_name = s3_client.generate_unique_filename(file.filename)
    s3_key = f'source/{s3_object_name}'
    file.file.seek(0)
    try:
        await s3_client.upload_file(file.file, s3_key, content_type)
    except S3Exception as e:
        raise HTTPException(
            status_code=HTTPStatus.BAD_GATEWAY,
            detail=f"Failed to upload video to s3: {str(e)}"
        )

    db_task = Task(
        source_file=s3_key,
        source_size=file.size,
        status=TaskStatus.PENDING,
    )
    db.add(db_task)
    await db.commit()
    await db.refresh(db_task)
    chain = (
        feature_calculator_task.s(db_task.id, db_task.source_file) |
        transcode_video_task.s(db_task.id)
    )
    await asyncio.get_event_loop().run_in_executor(None, chain)
    return TaskResponse.model_validate(db_task)


@app.get('/tasks')
async def list_tasks(
    db: DBSession,
    statuses: Annotated[list[TaskStatus] | None, Query()] = None,
    limit: int = 100,
    skip: int = 0,
) -> schemas.TaskListResponse:
    """
    Get a list of all active encoding tasks
    """
    # Get tasks that are pending or processing
    statement = select(Task)
    if statuses:
        statement = statement.filter(Task.status.in_(statuses))
    statement = statement.order_by(desc(Task.created_at)).offset(skip).limit(limit)
    result = await db.execute(statement)
    return schemas.TaskListResponse(
        tasks=[
            schemas.TaskResponse.model_validate(item)
            for item in result.scalars().all()
        ]
    )


@app.get('/tasks/{task_id}', response_model=schemas.TaskDetailResponse)
async def get_task(
    task_id: int,
    db: DBSession,
    s3_client: S3ClientAPI,
) -> schemas.TaskDetailResponse:
    """
    Get details of a specific task and a download URL if the task is completed
    """
    statement = select(Task).filter(Task.id == task_id)
    result = await db.execute(statement)
    task = result.scalars().first()
    if not task:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='Task not found')

    response = schemas.TaskDetailResponse.model_validate(task)

    # If the task is completed, generate a presigned URL for the output file
    if task.status == TaskStatus.COMPLETED and task.output_file:
        download_url = await s3_client.generate_presigned_url(task.output_file)
        response.download_url = download_url

    return response
