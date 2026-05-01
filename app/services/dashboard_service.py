from uuid import UUID

from fastapi import HTTPException
from sqlalchemy import select

from app.db.db import get_session
from app.dtos.dashboard import CreateDashboardRequest, CreateVisualizationRequest, UpdateVisualizationRequest
from app.models import Dashboard, Visualization
from app.utils.visualization_helpers import get_special_field_default_values


async def add(request: CreateDashboardRequest) -> Dashboard:
    visualizations = [Visualization(**v.model_dump()) for v in request.visualizations]
    dashboard = Dashboard(title=request.title, visualizations=visualizations)
    async with get_session() as session:
        session.add(dashboard)
        await session.commit()
        await session.refresh(dashboard)
    return dashboard


async def delete_by_uuid(uuid: UUID) -> None:
    async with get_session() as session:
        result = await session.execute(select(Dashboard).where(Dashboard.id == uuid))
        dashboard = result.scalar_one_or_none()
        if dashboard:
            await session.delete(dashboard)
            await session.commit()


async def find_many() -> list[Dashboard]:
    async with get_session() as session:
        result = await session.execute(select(Dashboard))
        return list(result.scalars().all())


async def find_by_uuid(uuid: UUID) -> Dashboard:
    async with get_session() as session:
        result = await session.execute(select(Dashboard).where(Dashboard.id == uuid))
        dashboard = result.scalar_one_or_none()
        if dashboard is None:
            raise HTTPException(status_code=404, detail="This instance was not found!")
        return dashboard


async def add_visualization(dashboard: Dashboard, visualization: CreateVisualizationRequest) -> Dashboard:
    vis = Visualization(**visualization.model_dump())
    vis.special_fields = get_special_field_default_values(vis)
    current = list(dashboard.visualizations)
    current.append(vis)
    async with get_session() as session:
        dashboard = await session.merge(dashboard)
        dashboard.visualizations = current
        await session.commit()
        await session.refresh(dashboard)
    return dashboard


async def remove_visualization(dashboard: Dashboard, visualization_uuid: UUID) -> Dashboard:
    current = list(dashboard.visualizations)
    updated = [v for v in current if v.uuid != str(visualization_uuid)]
    if len(updated) == len(current):
        raise HTTPException(status_code=404, detail="Visualization not found")
    async with get_session() as session:
        dashboard = await session.merge(dashboard)
        dashboard.visualizations = updated
        await session.commit()
        await session.refresh(dashboard)
    return dashboard


async def update_visualization(
    dashboard: Dashboard, visualization_uuid: UUID, request: UpdateVisualizationRequest
) -> Visualization:
    current = list(dashboard.visualizations)
    try:
        idx = next(i for i, v in enumerate(current) if v.uuid == str(visualization_uuid))
    except StopIteration:
        raise HTTPException(status_code=404, detail="Visualization not found")
    current[idx] = current[idx].model_copy(update=request.model_dump())
    async with get_session() as session:
        dashboard = await session.merge(dashboard)
        dashboard.visualizations = current
        await session.commit()
        await session.refresh(dashboard)
    return current[idx]