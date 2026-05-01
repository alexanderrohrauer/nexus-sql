import logging

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.db.db import get_session
from app.models import Work, Researcher, Institution, Affiliation

logger = logging.getLogger("uvicorn.error")

_SKIP_COLS = {"id", "imported_at"}


def _merge_columns(target, source, model_class) -> None:
    """Copy non-None fields from source onto target where target field is None."""
    for col in model_class.__table__.columns:
        if col.name in _SKIP_COLS:
            continue
        target_val = getattr(target, col.name)
        source_val = getattr(source, col.name)
        if target_val is None and source_val is not None:
            setattr(target, col.name, source_val)


async def merge_institutions(i1: Institution, i2: Institution) -> Institution:
    logger.info(f"Merging institutions '{i1.name}' and '{i2.name}'")
    merged_ext = i2.external_id.model_copy(update=i1.external_id.model_dump(exclude_none=True))
    async with get_session() as session:
        i1 = await session.merge(i1)
        i2 = await session.merge(i2)
        _merge_columns(i1, i2, Institution)
        i1.external_id = merged_ext
        i1.marked_for_removal = False

        aff_result = await session.execute(
            select(Affiliation).where(Affiliation.institution_id == i2.id)
        )
        for affiliation in aff_result.scalars().all():
            affiliation.institution_id = i1.id

        res_result = await session.execute(
            select(Researcher).where(Researcher.institution_id == i2.id)
        )
        for researcher in res_result.scalars().all():
            researcher.institution_id = i1.id

        i2.marked_for_removal = True
        await session.commit()
        await session.refresh(i1)
    return i1


async def merge_researchers(r1: Researcher, r2: Researcher) -> Researcher:
    logger.info(f"Merging researchers '{r1.full_name}' and '{r2.full_name}'")
    merged_ext = r2.external_id.model_copy(update=r1.external_id.model_dump(exclude_none=True))
    async with get_session() as session:
        r1 = await session.merge(r1)
        r2 = await session.merge(r2)
        _merge_columns(r1, r2, Researcher)
        r1.external_id = merged_ext
        if r1.institution_id is None:
            r1.institution_id = r2.institution_id
        r1.marked_for_removal = False

        work_result = await session.execute(
            select(Work).options(selectinload(Work.authors)).join(
                Work.authors
            ).where(Researcher.id == r2.id)
        )
        for work in work_result.scalars().all():
            work.replace_author(r2, r1)

        r2.marked_for_removal = True
        await session.commit()
        await session.refresh(r1)
    return r1


async def merge_works(w1: Work, w2: Work) -> Work:
    logger.info(f"Merging works '{w1.title}' and '{w2.title}'")
    merged_ext = w2.external_id.model_copy(update=w1.external_id.model_dump(exclude_none=True))
    merged_type = w2.type.model_copy(update=w1.type.model_dump(exclude_none=True))
    async with get_session() as session:
        w1 = await session.merge(w1)
        w2 = await session.merge(w2)
        # Load authors eagerly before merging
        await session.refresh(w1, ["authors"])
        await session.refresh(w2, ["authors"])

        _merge_columns(w1, w2, Work)
        w1.external_id = merged_ext
        w1.type = merged_type

        merged_authors = []
        for a1, a2 in zip(w1.authors or [], w2.authors or []):
            merged = await merge_researchers(a1, a2)
            merged = await session.merge(merged)
            merged_authors.append(merged)
        w1.authors = merged_authors

        w1.marked_for_removal = False
        w2.marked_for_removal = True
        await session.commit()
        await session.refresh(w1)
    return w1