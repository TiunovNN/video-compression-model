from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

import database
from settings import Settings, get_settings

APISettings = Annotated[Settings, Depends(get_settings)]


async def get_db(settings: APISettings) -> AsyncSession:
    async with database.async_session(settings) as session:
        yield session


DBSession = Annotated[AsyncSession, Depends(get_db)]
