from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.settings import get_settings

settings = get_settings()

jobstores = {
    "default": SQLAlchemyJobStore(url=settings.db_connection_string_sync)
}

scheduler = AsyncIOScheduler(jobstores=jobstores)