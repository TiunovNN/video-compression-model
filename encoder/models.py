from enum import StrEnum
from typing import Optional

from sqlalchemy import Enum, Identity, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Status(StrEnum):
    ENQUEUED = 'enqueued'
    IN_PROGRESS = 'in progress'
    FAILED = 'failed'
    SUCESS = 'success'


class EncoderTask(Base):
    __tablename__ = 'encoder_tasks'

    pk: Mapped[int] = mapped_column(
        Identity(start=0, minvalue=0, cycle=True),
        primary_key=True,
        index=True,
    )
    source_url: Mapped[str] = mapped_column(String(1024), nullable=False)
    destination_url: Mapped[str] = mapped_column(String(1024), nullable=False)
    crf: Mapped[Optional[int]] = mapped_column(nullable=True)
    qp: Mapped[Optional[int]] = mapped_column(nullable=True)
    status: Mapped[Status] = mapped_column(nullable=True, default=Status.ENQUEUED)
    details: Mapped[str] = mapped_column(Text, nullable=True)
