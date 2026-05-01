import logging
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.db.db import get_session
from app.models import Work, Researcher
from app.models.institutions import Institution

WORKS_AUTHORS_MAX_LENGTH = 30

logger = logging.getLogger("uvicorn.error")


async def insert_many(works: list[Work]) -> None:
    async with get_session() as session:
        for work in works:
            if work.authors is None or len(work.authors) <= WORKS_AUTHORS_MAX_LENGTH:
                session.add(work)
            else:
                logger.error(f"Error while inserting work {work.external_id}: too many authors")
        await session.commit()


async def find_by_id(uuid: UUID, with_relations: bool = False) -> Work:
    async with get_session() as session:
        stmt = select(Work).where(Work.id == uuid)
        if with_relations:
            stmt = stmt.options(
                selectinload(Work.authors).selectinload(Researcher.institution)
            )
        result = await session.execute(stmt)
        return result.scalar_one_or_none()


async def find_duplicates(uuid: UUID) -> list[Work]:
    entity = await find_by_id(uuid)
    if entity is None or entity.duplication_key is None:
        return []
    async with get_session() as session:
        stmt = select(Work).where(
            Work.duplication_key == entity.duplication_key,
            Work.id != entity.id,
        )
        result = await session.execute(stmt)
        return list(result.scalars().all())


async def mark_for_removal(uuid: UUID, uuids: list[UUID]) -> None:
    duplicates = await find_duplicates(uuid)
    async with get_session() as session:
        for duplicate in duplicates:
            duplicate = await session.merge(duplicate)
            duplicate.marked_for_removal = duplicate.id in uuids
        await session.commit()