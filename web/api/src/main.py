import asyncio
from http import HTTPStatus
from typing import Annotated

from fastapi import FastAPI, HTTPException, Query, UploadFile
from sqlalchemy import select

import schemas
from database import Task, TaskStatus
from deps import DBSession, S3ClientAPI, TranscodeVideoTaskAPI
from s3_client import S3Exception
from schemas import TaskResponse

app = FastAPI(title="Video Encoding Service")


@app.post('/tasks', status_code=HTTPStatus.CREATED)
async def create_encoding_task(
    db: DBSession,
    file: UploadFile,
    s3_client: S3ClientAPI,
    transcode_video_task: TranscodeVideoTaskAPI,
) -> schemas.TaskResponse:
    """
    Upload a video file and create an encoding task
    """
    # Check if file is a video
    content_type = file.content_type
    if not content_type.startswith("video/"):
        raise HTTPException(status_code=400, detail="File must be a video")

    # Upload to S3
    s3_object_name = s3_client.generate_unique_filename(file.filename)
    s3_key = f"source/{s3_object_name}"
    try:
        await s3_client.upload_file(file.file, s3_key)
    except S3Exception as e:
        raise HTTPException(
            status_code=HTTPStatus.BAD_GATEWAY,
            detail=f"Failed to upload video to s3: {str(e)}"
        )

    db_task = Task(source_file=s3_key, status=TaskStatus.PENDING)
    db.add(db_task)
    await db.commit()
    await db.refresh(db_task)
    await asyncio.get_event_loop().run_in_executor(
        None,
        transcode_video_task.delay,
        db_task.id,
    )
    print(f'{db_task.created_at=}')
    print(f'{db_task.created_at.tzinfo=}')
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
    statement = statement.order_by(Task.created_at).offset(skip).limit(limit)
    result = await db.execute(statement)
    return schemas.TaskListResponse(tasks=[
        schemas.TaskResponse.model_validate(item)
        for item in result.scalars().all()
    ])


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
