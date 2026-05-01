from uuid import UUID

from sqlalchemy import select

from app.db.db import get_session
from app.models import Institution
from app.settings import get_settings

settings = get_settings()


async def insert_many(institutions: list[Institution]) -> None:
    async with get_session() as session:
        for i, institution in enumerate(institutions):
            existing = None
            if settings.optimized_insert and institution.external_id_openalex:
                stmt = select(Institution).where(
                    Institution.external_id_openalex == institution.external_id_openalex
                )
                result = await session.execute(stmt)
                existing = result.scalar_one_or_none()
            if existing is None:
                session.add(institution)
            else:
                institutions[i] = existing
        await session.commit()


async def find_by_id(uuid: UUID) -> Institution:
    async with get_session() as session:
        result = await session.execute(select(Institution).where(Institution.id == uuid))
        return result.scalar_one_or_none()


async def find_duplicates(uuid: UUID) -> list[Institution]:
    entity = await find_by_id(uuid)
    if entity is None or entity.duplication_key is None:
        return []
    async with get_session() as session:
        stmt = select(Institution).where(
            Institution.duplication_key == entity.duplication_key,
            Institution.id != entity.id,
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