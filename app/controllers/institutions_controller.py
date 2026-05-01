import json
import logging
from typing import Annotated, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.db import get_session_dep
from app.dtos.duplications import MarkDuplicates
from app.dtos.institutions import InstitutionSearchParams
from app.dtos.visualizations import VisualizationData
from app.models import Institution
from app.services import institutions_service
from app.utils.visualization_helpers import parse_visualization_data
from app.visualizations import CHARTS

router = APIRouter(prefix="/institutions", tags=["institution"])

logger = logging.getLogger("uvicorn.error")


@router.get("")
async def get_institutions(
    params: Annotated[InstitutionSearchParams, Depends(InstitutionSearchParams)],
    session: AsyncSession = Depends(get_session_dep),
) -> list[Institution]:
    conditions = params.get_conditions(Institution)
    order_by = params.get_order_by(Institution)
    stmt = (
        select(Institution)
        .where(*conditions)
        .order_by(*order_by)
        .limit(params.limit)
        .offset(params.offset)
    )
    result = await session.execute(stmt)
    return list(result.scalars().all())


@router.get("/{uuid}")
async def get_institution(uuid: UUID) -> Institution:
    institution = await institutions_service.find_by_id(uuid)
    if institution is None:
        raise HTTPException(status_code=404, detail="Institution not found")
    return institution


@router.get("/{uuid}/duplicates")
async def get_institution_duplicates(uuid: UUID) -> list[Institution]:
    return await institutions_service.find_duplicates(uuid)


@router.get("/{uuid}/visualizations/{chart_identifier}")
async def get_institution_visualization_data(
    uuid: UUID, chart_identifier: str, q: Optional[str] = "{}"
) -> VisualizationData:
    institution = await institutions_service.find_by_id(uuid)
    try:
        chart_cls = next(c for c in CHARTS if c.identifier == chart_identifier)
        return await parse_visualization_data(chart_cls, json.loads(q), {}, institution=institution)
    except StopIteration:
        raise HTTPException(status_code=404, detail="Visualization not found")


@router.put("/{uuid}/mark-for-removal")
async def mark_institution_duplicates(uuid: UUID, dto: MarkDuplicates):
    return await institutions_service.mark_for_removal(uuid, dto.uuids)