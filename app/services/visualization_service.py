from uuid import UUID

from fastapi import HTTPException

from app.dtos.visualizations import VisualizationData
from app.models import Dashboard
from app.utils.visualization_helpers import parse_visualization_data
from app.visualizations import CHARTS


async def get_visualization_data(dashboard: Dashboard, visualization_uuid: UUID, queries: dict) -> VisualizationData:
    try:
        visualization = next(v for v in dashboard.visualizations if v.uuid == str(visualization_uuid))
        chart_cls = next(chart for chart in CHARTS if chart.identifier == visualization.chart)
        return await parse_visualization_data(chart_cls, queries, visualization.query_preset, special_fields=visualization.special_fields)
    except StopIteration:
        raise HTTPException(status_code=404, detail="Visualization not found")
