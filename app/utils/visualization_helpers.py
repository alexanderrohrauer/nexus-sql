from fastapi import HTTPException

from app.dtos.visualizations import VisualizationData
from app.models import Visualization
from app.utils.visualization_utils import ChartInput
from app.visualizations import MixedWorkAggregation, MixedActivityYearsTypes
from app.visualizations.mixed import MixedInstitutionAggregation


async def parse_visualization_data(chart_cls, queries: dict, query_preset: dict, **kwargs):
    try:
        chart_instance = chart_cls()
        special_fields = kwargs.pop("special_fields", {})
        chart_input = ChartInput(
            queries=queries, pre_filters=query_preset, special_fields=special_fields, **kwargs
        )
        return VisualizationData(
            series=await chart_instance.get_series(chart_input),
            generator=chart_instance.generator,
            chart_template=chart_instance.chart_template,
            filters=chart_input.queries,
        )
    except StopIteration:
        raise HTTPException(status_code=404, detail="Visualization-type not found")


def get_special_field_default_values(visualization: Visualization) -> dict:
    if visualization.chart == MixedInstitutionAggregation.identifier:
        return {MixedInstitutionAggregation.INSTITUTION_FIELD_NAME: "type"}
    if visualization.chart == MixedWorkAggregation.identifier:
        return {MixedWorkAggregation.WORK_FIELD_NAME: "publication_year"}
    if visualization.chart == MixedActivityYearsTypes.identifier:
        return {MixedActivityYearsTypes.TYPE_FIELD_NAME: "openalex_type"}
    return {}
