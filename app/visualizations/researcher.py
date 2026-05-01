from itertools import combinations

import pydash as _
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.db.db import get_session
from app.models import Affiliation, Work, Researcher
from app.utils.visualization_utils import (
    Chart, ChartType, ChartTemplates, ChartInput, SeriesMap,
    create_basic_generator, EntityType, Series, read_generator,
)


class ResearcherAffiliations(Chart):
    identifier = "researcher_affiliations"
    name = "Researcher affiliations"
    type = ChartType.RESEARCHER
    chart_template = ChartTemplates.CUSTOM
    generator = create_basic_generator(["affiliations"])

    async def get_series(self, chart_input: ChartInput) -> SeriesMap:
        result = SeriesMap()
        researcher = chart_input.researcher
        conditions = chart_input.get_series_conditions(Affiliation, "affiliations")
        async with get_session() as session:
            stmt = (
                select(Affiliation)
                .options(selectinload(Affiliation.institution))
                .where(Affiliation.researcher_id == researcher.id, *conditions)
            )
            affiliations = (await session.execute(stmt)).scalars().all()
        result.add("affiliations", Series(data=[a.model_dump() for a in affiliations], entity_type=EntityType.AFFILIATIONS))
        return result


class ResearcherPerformance(Chart):
    identifier = "researcher_performance"
    name = "Performance"
    type = ChartType.RESEARCHER
    chart_template = ChartTemplates.ECHARTS
    generator = read_generator("researcherPerformance.js")

    async def get_series(self, chart_input: ChartInput) -> SeriesMap:
        result = SeriesMap()
        researcher = chart_input.researcher
        if researcher.openalex_meta and "summary_stats" in researcher.openalex_meta:
            stats = researcher.openalex_meta["summary_stats"]
            result.add("h_index", Series(data=stats["h_index"], entity_type=None))
            result.add("2yr_mean_citedness", Series(data=stats["2yr_mean_citedness"], entity_type=None))
            result.add("i10_index", Series(data=stats["i10_index"], entity_type=None))
        return result


class ResearcherRelationGraph(Chart):
    identifier = "researcher_relation_graph"
    name = "Relations"
    type = ChartType.RESEARCHER
    chart_template = ChartTemplates.ECHARTS
    generator = read_generator("researcherRelationGraph.js")

    def get_nodes_and_links(self, works, cat_func, size_func):
        nodes, links = [], []
        for w in works:
            author_nodes = [
                {"id": a.id, "name": a.full_name, "category": cat_func(a), "symbolSize": size_func(a),
                 "$nexus": {"type": EntityType.RESEARCHER, "id": a.id}}
                for a in (w.authors or [])
            ]
            nodes += author_nodes
            author_ids = [a.id for a in (w.authors or [])]
            links += [{"source": s, "target": t} for s, t in combinations(author_ids, 2)]
        return nodes, links

    async def get_series(self, chart_input: ChartInput) -> SeriesMap:
        result = SeriesMap()
        researcher = chart_input.researcher
        conditions = chart_input.get_series_conditions(Work, "works")
        async with get_session() as session:
            from app.models.works import work_authors
            stmt = (
                select(Work)
                .options(selectinload(Work.authors))
                .join(work_authors)
                .where(work_authors.c.researcher_id == researcher.id, *conditions)
            )
            l1 = (await session.execute(stmt)).scalars().all()
            nodes1, links1 = self.get_nodes_and_links(
                l1,
                lambda a: 0 if a.id == researcher.id else 1,
                lambda a: 60 if a.id == researcher.id else 25,
            )
            l2_ids = [n["id"] for n in nodes1 if n["category"] == 1]
            if l2_ids:
                stmt2 = (
                    select(Work)
                    .options(selectinload(Work.authors))
                    .join(work_authors)
                    .where(work_authors.c.researcher_id.in_(l2_ids), *conditions)
                )
                l2 = (await session.execute(stmt2)).scalars().all()
                nodes2, links2 = self.get_nodes_and_links(l2, lambda a: 2, lambda a: 10)
            else:
                nodes2, links2 = [], []

        nodes = _.uniq_by(nodes1 + nodes2, lambda a: a["id"])
        result.add("works", Series(data={"data": nodes, "links": links1 + links2}, entity_type=EntityType.WORK))
        return result