from enum import StrEnum

from sqlalchemy import Column, Enum, Identity, Integer, String, Text
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Status(StrEnum):
    ENQUEUED = 'enqueued'
    IN_PROGRESS = 'in progress'
    FAILED = 'failed'
    SUCESS = 'success'


class EncoderTask(Base):
    __tablename__ = 'encoder_tasks'

    pk = Column(
        Integer,
        Identity(start=0, minvalue=0, cycle=True),
        primary_key=True,
        index=True,
    )
    source_url = Column(String(1024))
    destination_url = Column(String(1024))
    crf = Column(Integer)
    qp = Column(Integer)
    status = Column(Enum(Status))
    details = Column(Text)
