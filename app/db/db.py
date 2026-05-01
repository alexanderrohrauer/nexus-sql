from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

from app.settings import get_settings

settings = get_settings()

engine = create_async_engine(settings.db_connection_string, echo=False)
async_session_factory = async_sessionmaker(engine, expire_on_commit=False)


async def init():
    from app.db.models import Base
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


@asynccontextmanager
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async with async_session_factory() as session:
        yield session


async def get_session_dep() -> AsyncGenerator[AsyncSession, None]:
    async with async_session_factory() as session:
        yield session