from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict

from database import TaskStatus


class TaskBase(BaseModel):
    model_config = ConfigDict(from_attributes=True)

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


class TaskListResponse(BaseModel):
    tasks: list[TaskResponse]


class TaskDetailResponse(TaskResponse):
    source_size: int
    output_size: Optional[int] = None
    download_url: Optional[str] = None
