from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from settings import Settings


def async_engine(settings: Settings):
    return create_async_engine(settings.DATABASE_URL, echo=True)


def async_session(settings: Settings) -> AsyncSession:
    return sessionmaker(
        bind=async_engine(settings),
        expire_on_commit=False,
        class_=AsyncSession,
    )()
