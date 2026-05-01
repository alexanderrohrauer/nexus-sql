import logging
from uuid import UUID

from sqlalchemy import select, distinct

from app.db.db import get_session
from app.models import Work, Researcher, Institution
from app.services import merge_service

logger = logging.getLogger("uvicorn.error")


async def _get_duplicate_keys(model_class) -> list[UUID]:
    async with get_session() as session:
        stmt = select(distinct(model_class.duplication_key)).where(
            model_class.duplication_key.isnot(None)
        )
        result = await session.execute(stmt)
        return [row[0] for row in result.all()]


async def eliminate_work_duplicates() -> None:
    logger.info("Work duplicate elimination started...")
    keys = await _get_duplicate_keys(Work)
    for key in keys:
        async with get_session() as session:
            result = await session.execute(
                select(Work).where(Work.duplication_key == key)
            )
            works = list(result.scalars().all())
        try:
            merged = next(w for w in works if not w.marked_for_removal)
        except StopIteration:
            merged = sorted(works, key=lambda w: w.imported_at)[0]
        should_clear_key = True
        for work in works:
            if work.id != merged.id:
                if work.marked_for_removal:
                    merged = await merge_service.merge_works(merged, work)
                else:
                    should_clear_key = False
        if should_clear_key:
            async with get_session() as session:
                m = await session.merge(merged)
                m.duplication_key = None
                await session.commit()
    logger.info("Work duplicate elimination finished...")


async def eliminate_researcher_duplicates() -> None:
    logger.info("Researcher duplicate elimination started...")
    keys = await _get_duplicate_keys(Researcher)
    for key in keys:
        async with get_session() as session:
            result = await session.execute(
                select(Researcher).where(Researcher.duplication_key == key)
            )
            researchers = list(result.scalars().all())
        try:
            merged = next(r for r in researchers if not r.marked_for_removal)
        except StopIteration:
            merged = sorted(researchers, key=lambda r: r.imported_at)[0]
        should_clear_key = True
        for researcher in researchers:
            if researcher.id != merged.id:
                if researcher.marked_for_removal:
                    merged = await merge_service.merge_researchers(merged, researcher)
                else:
                    should_clear_key = False
        if should_clear_key:
            async with get_session() as session:
                m = await session.merge(merged)
                m.duplication_key = None
                await session.commit()
    logger.info("Researcher duplicate elimination finished...")


async def eliminate_institutions_duplicates() -> None:
    logger.info("Institution duplicate elimination started...")
    keys = await _get_duplicate_keys(Institution)
    for key in keys:
        async with get_session() as session:
            result = await session.execute(
                select(Institution).where(Institution.duplication_key == key)
            )
            institutions = list(result.scalars().all())
        try:
            merged = next(i for i in institutions if not i.marked_for_removal)
        except StopIteration:
            merged = sorted(institutions, key=lambda i: i.imported_at)[0]
        should_clear_key = True
        for institution in institutions:
            if institution.id != merged.id:
                if institution.marked_for_removal:
                    merged = await merge_service.merge_institutions(merged, institution)
                else:
                    should_clear_key = False
        if should_clear_key:
            async with get_session() as session:
                m = await session.merge(merged)
                m.duplication_key = None
                await session.commit()
    logger.info("Institution duplicate elimination finished...")