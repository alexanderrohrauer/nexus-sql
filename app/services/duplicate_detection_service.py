from uuid import uuid4

import nltk
import pydash as _

from app.db.db import get_session
from app.models import Work, Researcher, Institution
from sqlalchemy import select, asc
from sqlalchemy.orm import selectinload

w_works = 5
w_researchers = 5
w_institutions = 5

WORKS_LEVENSHTEIN_THRESHOLD = 3
RESEARCHERS_LEVENSHTEIN_THRESHOLD = 3
INSTITUTIONS_LEVENSHTEIN_THRESHOLD = 3


async def deduplicate_works() -> None:
    skip = 0
    limit = 2
    works = None
    while works is None or len(works) > 0:
        print(f"Work batch: {skip}")
        async with get_session() as session:
            stmt = select(Work).order_by(asc(Work.snm_key)).offset(skip).limit(limit)
            result = await session.execute(stmt)
            works = list(result.scalars().all())
        if len(works) > 0:
            current_work = works[-1]
            duplication_key = current_work.duplication_key or uuid4()
            for prev_work in reversed(works[:-1]):
                if prev_work.id != current_work.id:
                    distance = nltk.edit_distance(prev_work.normalized_title, current_work.normalized_title)
                    id_match = prev_work.external_id.matches(current_work.external_id)
                    if distance <= WORKS_LEVENSHTEIN_THRESHOLD or id_match:
                        print(f"Duplicate work: {prev_work.title} / {current_work.title}")
                        async with get_session() as session:
                            pw = await session.merge(prev_work)
                            cw = await session.merge(current_work)
                            pw.duplication_key = duplication_key
                            pw.marked_for_removal = id_match or pw.marked_for_removal
                            cw.duplication_key = duplication_key
                            cw.marked_for_removal = id_match or cw.marked_for_removal
                            await session.commit()
                else:
                    break
        skip = skip + 1 if limit >= w_works else skip
        limit = limit + 1 if limit < w_works else limit


async def deduplicate_researchers() -> None:
    skip = 0
    limit = 2
    researchers = None
    while researchers is None or len(researchers) > 0:
        print(f"Researcher batch: {skip}")
        async with get_session() as session:
            stmt = (
                select(Researcher)
                .order_by(asc(Researcher.snm_key))
                .offset(skip)
                .limit(limit)
            )
            result = await session.execute(stmt)
            researchers = list(result.scalars().all())
        if len(researchers) > 0:
            current = researchers[-1]
            duplication_key = current.duplication_key or uuid4()
            for prev in reversed(researchers[:-1]):
                if prev.id != current.id:
                    distance = nltk.edit_distance(prev.normalized_full_name, current.normalized_full_name)
                    id_match = prev.external_id.matches(current.external_id)
                    distance_match = distance <= RESEARCHERS_LEVENSHTEIN_THRESHOLD
                    if distance_match:
                        async with get_session() as session:
                            prev_works_res = await session.execute(
                                select(Work).join(Work.authors).where(Researcher.id == prev.id)
                            )
                            curr_works_res = await session.execute(
                                select(Work).join(Work.authors).where(Researcher.id == current.id)
                            )
                            prev_dois = [w.external_id_doi for w in prev_works_res.scalars() if w.external_id_doi]
                            curr_dois = [w.external_id_doi for w in curr_works_res.scalars() if w.external_id_doi]
                        if _.duplicates(prev_dois + curr_dois):
                            id_match = True
                    if distance_match or id_match:
                        print(f"Duplicate researcher: {prev.full_name} / {current.full_name}")
                        async with get_session() as session:
                            p = await session.merge(prev)
                            c = await session.merge(current)
                            p.duplication_key = duplication_key
                            p.marked_for_removal = id_match or p.marked_for_removal
                            c.duplication_key = duplication_key
                            c.marked_for_removal = id_match or c.marked_for_removal
                            await session.commit()
                else:
                    break
        skip = skip + 1 if limit >= w_researchers else skip
        limit = limit + 1 if limit < w_researchers else limit


async def deduplicate_institutions() -> None:
    skip = 0
    limit = 2
    institutions = None
    while institutions is None or len(institutions) > 0:
        print(f"Institution batch: {skip}")
        async with get_session() as session:
            stmt = (
                select(Institution)
                .order_by(asc(Institution.snm_key))
                .offset(skip)
                .limit(limit)
            )
            result = await session.execute(stmt)
            institutions = list(result.scalars().all())
        if len(institutions) > 0:
            current = institutions[-1]
            duplication_key = current.duplication_key or uuid4()
            for prev in reversed(institutions[:-1]):
                if prev.id != current.id:
                    distance = nltk.edit_distance(prev.normalized_name, current.normalized_name)
                    id_match = prev.external_id.matches(current.external_id)
                    attribute_match = (
                        distance <= INSTITUTIONS_LEVENSHTEIN_THRESHOLD
                        and prev.city == current.city
                        and prev.country == current.country
                    )
                    if attribute_match or id_match:
                        print(f"Duplicate institution: {prev.name} / {current.name}")
                        async with get_session() as session:
                            p = await session.merge(prev)
                            c = await session.merge(current)
                            p.duplication_key = duplication_key
                            p.marked_for_removal = id_match or p.marked_for_removal
                            c.duplication_key = duplication_key
                            c.marked_for_removal = id_match or c.marked_for_removal
                            await session.commit()
                else:
                    break
        skip = skip + 1 if limit >= w_institutions else skip
        limit = limit + 1 if limit < w_institutions else limit