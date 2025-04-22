from datetime import UTC, datetime
from enum import StrEnum
from functools import partial
from typing import Optional

from sqlalchemy import DateTime, Identity, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql import func
from sqlalchemy_utc import utcnow, UtcDateTime


class Base(DeclarativeBase):
    pass


class TaskStatus(StrEnum):
    PENDING = 'pending'
    PROCESSING = 'processing'
    COMPLETED = 'completed'
    FAILED = 'failed'


class Task(Base):
    __tablename__ = "tasks"

    id: Mapped[int] = mapped_column(Identity(start=0, minvalue=0, cycle=True), primary_key=True, index=True)
    source_file: Mapped[str] = mapped_column(String(1024), nullable=False)
    output_file: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)
    status: Mapped[TaskStatus] = mapped_column(default=TaskStatus.PENDING, nullable=False)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        UtcDateTime(),
        server_default=utcnow()
    )
    updated_at: Mapped[datetime] = mapped_column(
        UtcDateTime(),
        server_default=utcnow(),
        onupdate=utcnow(),
    )
