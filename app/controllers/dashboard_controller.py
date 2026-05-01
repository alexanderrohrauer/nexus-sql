import json
from http import HTTPStatus
from typing import Optional
from uuid import UUID

from fastapi import APIRouter

from app.dtos.dashboard import CreateDashboardRequest, DashboardMinimal, CreateVisualizationRequest, \
    UpdateVisualizationRequest
from app.dtos.visualizations import VisualizationData
from app.models import Dashboard, Visualization
from app.services import dashboard_service, visualization_service

router = APIRouter(prefix="/dashboards", tags=["dashboard"])


@router.post("", status_code=HTTPStatus.CREATED)
async def create_dashboard(dashboard: CreateDashboardRequest) -> Dashboard:
    return await dashboard_service.add(dashboard)


@router.get("")
async def get_dashboards() -> list[DashboardMinimal]:
    return DashboardMinimal.from_model_list(await dashboard_service.find_many())


@router.get("/{uuid}")
async def get_dashboard(uuid: UUID) -> Dashboard:
    return await dashboard_service.find_by_uuid(uuid)


@router.delete("/{uuid}")
async def delete_dashboard(uuid: UUID) -> None:
    await dashboard_service.delete_by_uuid(uuid)


@router.get("/{uuid}/visualizations/{visualization_uuid}/data")
async def get_visualization_data(
    uuid: UUID, visualization_uuid: UUID, q: Optional[str] = "{}"
) -> VisualizationData:
    dashboard = await dashboard_service.find_by_uuid(uuid)
    return await visualization_service.get_visualization_data(dashboard, visualization_uuid, json.loads(q))


@router.post("/{uuid}/visualizations")
async def add_visualization(uuid: UUID, visualization: CreateVisualizationRequest) -> Dashboard:
    dashboard = await dashboard_service.find_by_uuid(uuid)
    return await dashboard_service.add_visualization(dashboard, visualization)


@router.put("/{uuid}/visualizations/{visualization_uuid}")
async def update_visualization(
    uuid: UUID, visualization_uuid: UUID, request: UpdateVisualizationRequest
) -> Visualization:
    dashboard = await dashboard_service.find_by_uuid(uuid)
    return await dashboard_service.update_visualization(dashboard, visualization_uuid, request)


@router.delete("/{uuid}/visualizations/{visualization_uuid}")
async def remove_visualization(uuid: UUID, visualization_uuid: UUID) -> Dashboard:
    dashboard = await dashboard_service.find_by_uuid(uuid)
    return await dashboard_service.remove_visualization(dashboard, visualization_uuid)