from datetime import datetime
from typing import Optional

from pydantic import BaseModel

from database import TaskStatus


class TaskBase(BaseModel):
    source_file: str


class TaskCreate(TaskBase):
    pass


class TaskResponse(TaskBase):
    id: int
    status: TaskStatus
    output_file: Optional[str] = None
    error_message: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        orm_mode = True


class TaskListResponse(BaseModel):
    tasks: list[TaskResponse]


class TaskDetailResponse(TaskResponse):
    download_url: Optional[str] = None
