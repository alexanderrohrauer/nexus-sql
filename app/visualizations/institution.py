from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.db.db import get_session
from app.models import Researcher
from app.utils.visualization_utils import (
    Chart, ChartType, ChartTemplates, ChartInput, SeriesMap,
    create_basic_generator, EntityType, Series,
)


class InstitutionCurrentResearchers(Chart):
    identifier = "institution_current_researchers"
    name = "Current researchers"
    type = ChartType.INSTITUTION
    chart_template = ChartTemplates.CUSTOM
    generator = create_basic_generator(["researchers"])

    async def get_series(self, chart_input: ChartInput) -> SeriesMap:
        result = SeriesMap()
        institution = chart_input.institution
        conditions = chart_input.get_series_conditions(Researcher, "researchers")
        async with get_session() as session:
            stmt = (
                select(Researcher)
                .options(selectinload(Researcher.institution))
                .where(Researcher.institution_id == institution.id, *conditions)
            )
            researchers = (await session.execute(stmt)).scalars().all()
        result.add("researchers", Series(data=[r.model_dump() for r in researchers], entity_type=EntityType.RESEARCHER))
        return result
