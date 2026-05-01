import json
import logging
from typing import Annotated, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.db.db import get_session_dep
from app.dtos.duplications import MarkDuplicates
from app.dtos.visualizations import VisualizationData
from app.dtos.works import WorkSearchParams
from app.models import Work, Researcher
from app.services import works_service
from app.utils.visualization_helpers import parse_visualization_data
from app.visualizations import CHARTS

router = APIRouter(prefix="/works", tags=["work"])

logger = logging.getLogger("uvicorn.error")


@router.get("")
async def get_works(
    params: Annotated[WorkSearchParams, Depends(WorkSearchParams)],
    session: AsyncSession = Depends(get_session_dep),
) -> list[Work]:
    conditions = params.get_conditions(Work)
    order_by = params.get_order_by(Work)
    stmt = (
        select(Work)
        .options(selectinload(Work.authors).selectinload(Researcher.institution))
        .where(*conditions)
        .order_by(*order_by)
        .limit(params.limit)
        .offset(params.offset)
    )
    result = await session.execute(stmt)
    return list(result.scalars().all())


@router.get("/{uuid}")
async def get_work(uuid: UUID) -> Work:
    work = await works_service.find_by_id(uuid, with_relations=True)
    if work is None:
        raise HTTPException(status_code=404, detail="Work not found")
    return work


@router.get("/{uuid}/duplicates")
async def get_work_duplicates(uuid: UUID) -> list[Work]:
    return await works_service.find_duplicates(uuid)


@router.get("/{uuid}/visualizations/{chart_identifier}")
async def get_work_visualization_data(
    uuid: UUID, chart_identifier: str, q: Optional[str] = "{}"
) -> VisualizationData:
    work = await works_service.find_by_id(uuid, with_relations=True)
    try:
        chart_cls = next(c for c in CHARTS if c.identifier == chart_identifier)
        return await parse_visualization_data(chart_cls, json.loads(q), {}, work=work)
    except StopIteration:
        raise HTTPException(status_code=404, detail="Visualization not found")


@router.put("/{uuid}/mark-for-removal")
async def mark_work_duplicates(uuid: UUID, dto: MarkDuplicates):
    return await works_service.mark_for_removal(uuid, dto.uuids)