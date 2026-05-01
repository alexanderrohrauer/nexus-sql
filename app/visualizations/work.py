from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.db.db import get_session
from app.models import Researcher
from app.models.works import work_authors
from app.utils.visualization_utils import (
    Chart, ChartType, ChartTemplates, ChartInput, SeriesMap,
    create_basic_generator, EntityType, Series,
)


class WorkAuthors(Chart):
    identifier = "work_authors"
    name = "Authors"
    type = ChartType.WORK
    chart_template = ChartTemplates.CUSTOM
    generator = create_basic_generator(["authors"])

    async def get_series(self, chart_input: ChartInput) -> SeriesMap:
        result = SeriesMap()
        work = chart_input.work
        conditions = chart_input.get_series_conditions(Researcher, "authors")
        async with get_session() as session:
            author_ids = [a.id for a in (work.authors or [])]
            if not author_ids:
                result.add("authors", Series(data=[], entity_type=EntityType.RESEARCHER))
                return result
            stmt = (
                select(Researcher)
                .options(selectinload(Researcher.institution))
                .where(Researcher.id.in_(author_ids), *conditions)
            )
            researchers = (await session.execute(stmt)).scalars().all()
        result.add("authors", Series(data=[r.model_dump() for r in researchers], entity_type=EntityType.RESEARCHER))
        return result
