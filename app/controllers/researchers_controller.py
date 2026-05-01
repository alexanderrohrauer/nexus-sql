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
from app.dtos.researchers import ResearcherSearchParams
from app.dtos.visualizations import VisualizationData
from app.models import Researcher, Affiliation
from app.services import researchers_service
from app.utils.visualization_helpers import parse_visualization_data
from app.visualizations import CHARTS

router = APIRouter(prefix="/researchers", tags=["researcher"])

logger = logging.getLogger("uvicorn.error")


@router.get("")
async def get_researchers(
    params: Annotated[ResearcherSearchParams, Depends(ResearcherSearchParams)],
    session: AsyncSession = Depends(get_session_dep),
) -> list[Researcher]:
    conditions = params.get_conditions(Researcher)
    order_by = params.get_order_by(Researcher)
    stmt = (
        select(Researcher)
        .options(
            selectinload(Researcher.institution),
            selectinload(Researcher.affiliations).selectinload(Affiliation.institution),
        )
        .where(*conditions)
        .order_by(*order_by)
        .limit(params.limit)
        .offset(params.offset)
    )
    result = await session.execute(stmt)
    return list(result.scalars().all())


@router.get("/{uuid}")
async def get_researcher(uuid: UUID) -> Researcher:
    researcher = await researchers_service.find_by_id(uuid, with_relations=True)
    if researcher is None:
        raise HTTPException(status_code=404, detail="Researcher not found")
    return researcher


@router.get("/{uuid}/duplicates")
async def get_researcher_duplicates(uuid: UUID) -> list[Researcher]:
    return await researchers_service.find_duplicates(uuid)


@router.get("/{uuid}/visualizations/{chart_identifier}")
async def get_researcher_visualization_data(
    uuid: UUID, chart_identifier: str, q: Optional[str] = "{}"
) -> VisualizationData:
    researcher = await researchers_service.find_by_id(uuid, with_relations=True)
    try:
        chart_cls = next(c for c in CHARTS if c.identifier == chart_identifier)
        return await parse_visualization_data(chart_cls, json.loads(q), {}, researcher=researcher)
    except StopIteration:
        raise HTTPException(status_code=404, detail="Visualization not found")


@router.put("/{uuid}/mark-for-removal")
async def mark_researcher_duplicates(uuid: UUID, dto: MarkDuplicates):
    return await researchers_service.mark_for_removal(uuid, dto.uuids)