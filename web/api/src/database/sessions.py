import logging

from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from settings import Settings


def async_engine(settings: Settings):
    url = make_url(settings.DATABASE_URL)
    url = url.set(drivername='postgresql+asyncpg')
    logging.info(f'Connection to DB {url.render_as_string()}')
    return create_async_engine(url.render_as_string(False))


def async_session(settings: Settings) -> AsyncSession:
    return sessionmaker(
        bind=async_engine(settings),
        expire_on_commit=False,
        class_=AsyncSession,
    )()
