from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.db.db import get_session
from app.models import Researcher, Affiliation


async def insert_many(researchers: list[Researcher]) -> None:
    async with get_session() as session:
        for researcher in researchers:
            session.add(researcher)
        await session.commit()


async def find_by_id(uuid: UUID, with_relations: bool = False) -> Researcher:
    async with get_session() as session:
        stmt = select(Researcher).where(Researcher.id == uuid)
        if with_relations:
            stmt = stmt.options(
                selectinload(Researcher.institution),
                selectinload(Researcher.affiliations).selectinload(Affiliation.institution),
            )
        result = await session.execute(stmt)
        return result.scalar_one_or_none()


async def find_duplicates(uuid: UUID) -> list[Researcher]:
    entity = await find_by_id(uuid)
    if entity is None or entity.duplication_key is None:
        return []
    async with get_session() as session:
        stmt = select(Researcher).where(
            Researcher.duplication_key == entity.duplication_key,
            Researcher.id != entity.id,
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