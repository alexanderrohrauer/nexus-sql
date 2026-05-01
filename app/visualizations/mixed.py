import numpy as np
import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.db.db import get_session
from app.models import Work, Institution, Researcher
from app.utils.db_utils import fix_location_util
from app.utils.visualization_utils import (
    Chart, ChartType, ChartTemplates, read_generator, SeriesMap, ChartInput,
    Series, EntityType, create_basic_generator,
)


class InstitutionsMap(Chart):
    identifier = "institutions_map"
    name = "Institutions map"
    type = ChartType.MIXED
    chart_template = ChartTemplates.LEAFLET
    generator = create_basic_generator(["institutions"])

    async def get_series(self, chart_input: ChartInput, **kwargs) -> SeriesMap:
        result = SeriesMap()
        conditions = chart_input.get_series_conditions(Institution, "institutions")
        async with get_session() as session:
            stmt = select(Institution).where(
                Institution.location.isnot(None), *conditions
            )
            res = await session.execute(stmt)
            institutions = res.scalars().all()
        icon = kwargs.get("icon") or "institution.png"
        series = {
            "type": "marker",
            "showAtZoom": kwargs.get("show_at_zoom"),
            "data": [
                {
                    "id": i.id, "name": i.name,
                    "position": fix_location_util(i.location),
                    "icon": icon,
                    "$nexus": {"type": EntityType.INSTITUTION, "id": i.id},
                }
                for i in institutions
            ],
        }
        result.add("institutions", Series(data=series, entity_type=EntityType.INSTITUTION))
        return result


class ResearcherMap(Chart):
    identifier = "researcher_map"
    name = "Researcher map"
    type = ChartType.MIXED
    chart_template = ChartTemplates.LEAFLET
    generator = create_basic_generator(["researchers"])

    async def get_series(self, chart_input: ChartInput, **kwargs) -> SeriesMap:
        result = SeriesMap()
        conditions = chart_input.get_series_conditions(Researcher, "researchers")
        async with get_session() as session:
            stmt = (
                select(Researcher)
                .options(selectinload(Researcher.institution))
                .where(Researcher.institution_id.isnot(None), *conditions)
            )
            res = await session.execute(stmt)
            researchers = res.scalars().all()
        icon = kwargs.get("icon") or "researcher.png"
        series = {
            "type": "marker",
            "showAtZoom": kwargs.get("show_at_zoom"),
            "data": [
                {
                    "id": r.id, "name": r.full_name,
                    "position": fix_location_util(r.institution.location if r.institution else None),
                    "icon": icon,
                    "$nexus": {"type": EntityType.RESEARCHER, "id": r.id},
                }
                for r in researchers
                if r.institution is not None and r.institution.location is not None
            ],
        }
        result.add("researchers", Series(data=series, entity_type=EntityType.RESEARCHER))
        return result


class WorksGeoHeatmap(Chart):
    identifier = "works_geo_heatmap"
    name = "Works Geo-Heatmap"
    type = ChartType.MIXED
    chart_template = ChartTemplates.LEAFLET
    generator = create_basic_generator(["works", "institutions"])

    async def get_series(self, chart_input: ChartInput) -> SeriesMap:
        result = SeriesMap()
        conditions = chart_input.get_series_conditions(Work, "works")
        async with get_session() as session:
            stmt = (
                select(Work)
                .options(selectinload(Work.authors).selectinload(Researcher.institution))
                .where(*conditions)
            )
            res = await session.execute(stmt)
            works = res.scalars().all()

        lng_lat_map = {}
        for work in works:
            for author in work.authors or []:
                if author.institution is not None and author.institution.location is not None:
                    from geoalchemy2.shape import to_shape
                    pt = to_shape(author.institution.location)
                    key = f"{pt.x},{pt.y}"
                    if key in lng_lat_map:
                        lng_lat_map[key][-1] += 1
                    else:
                        lng_lat_map[key] = [pt.y, pt.x, 1]

        heatmap_data = list(lng_lat_map.values())
        np_data = np.array(heatmap_data) if heatmap_data else np.array([])
        series_data = []
        q1 = q2 = q3 = 1
        if len(np_data) > 0:
            series = pd.Series(np_data[:, 2])
            scaled = series / series.abs().max()
            np_data[:, 2] = scaled
            q1 = float(np.percentile(scaled, 25))
            q2 = float(np.percentile(scaled, 50))
            q3 = float(np.percentile(scaled, 80))
            series_data = np_data.tolist()

        data = {"type": "heatmap", "data": {"data": series_data, "gradient": {q1: "blue", q2: "lime", q3: "red"}, "radius": 10, "minOpacity": 0.2}}
        result.add("works", Series(data=data, entity_type=EntityType.WORK))

        institution_map = await InstitutionsMap().get_series(chart_input, icon="dot.svg", show_at_zoom=3)
        return result + institution_map


class TopResearcherWorksCount(Chart):
    identifier = "top_researcher_works_count"
    name = "Top Researcher works count"
    type = ChartType.MIXED
    chart_template = ChartTemplates.ECHARTS
    generator = read_generator("topResearcherWorksCount.js")

    async def get_series(self, chart_input: ChartInput) -> SeriesMap:
        result = SeriesMap()
        conditions = chart_input.get_series_conditions(Researcher, "researchers")
        async with get_session() as session:
            stmt = select(Researcher).where(
                Researcher.openalex_meta.isnot(None), *conditions
            )
            res = await session.execute(stmt)
            researchers = res.scalars().all()
        points = [
            [r.full_name, {"value": r.openalex_meta["works_count"], "$nexus": {"type": EntityType.RESEARCHER, "id": r.id}}]
            for r in researchers if r.openalex_meta
        ]
        points = list(reversed(sorted(points, key=lambda row: row[-1]["value"])))[:20]
        result.add("researchers", Series(data=points, entity_type=EntityType.RESEARCHER))
        return result


class SummaryChart(Chart):
    identifier = "basic_stats"
    name = "Summary chart"
    type = ChartType.MIXED
    chart_template = ChartTemplates.MARKDOWN
    generator = create_basic_generator(["researchers", "institutions", "works"])

    async def get_series(self, chart_input: ChartInput) -> SeriesMap:
        result = SeriesMap()
        async with get_session() as session:
            works_conditions = chart_input.get_series_conditions(Work, "works")
            researcher_conditions = chart_input.get_series_conditions(Researcher, "researchers")
            institution_conditions = chart_input.get_series_conditions(Institution, "institutions")

            works = (await session.execute(select(Work).where(*works_conditions))).scalars().all()
            researchers = (await session.execute(select(Researcher).where(*researcher_conditions))).scalars().all()
            institutions = (await session.execute(select(Institution).where(*institution_conditions))).scalars().all()

        works_df = pd.DataFrame([w.model_dump() for w in works])
        highest_keywords = works_df.explode("keywords")["keywords"].value_counts().head(3) if not works_df.empty else pd.Series()

        researchers_df = pd.DataFrame([r.model_dump() for r in researchers])
        if not researchers_df.empty:
            researchers_df["works_count"] = researchers_df["openalex_meta"].apply(
                lambda m: m["works_count"] if m else None
            )
            top_researchers = researchers_df.nlargest(3, "works_count", keep="all")
            highest_researchers = [
                f'<a href="/researchers/{r["uuid"]}" target="_blank">{r["full_name"]}</a>'
                for r in top_researchers.to_dict(orient="records")
            ]
            researchers_df["h_index"] = researchers_df["openalex_meta"].apply(
                lambda m: m["summary_stats"]["h_index"] if m else None
            )
            h_idx_max = researchers_df["h_index"].dropna()
            highest_h_score = researchers_df.loc[h_idx_max.idxmax()] if not h_idx_max.empty else None
        else:
            highest_researchers = []
            highest_h_score = None

        result.add("works", Series(data=f"""### Works\n**Total count:** {len(works)}\n\n**Most used keywords:** {", ".join(highest_keywords.index) if not highest_keywords.empty else "N/A"}""", entity_type=EntityType.WORK))
        result.add("researchers", Series(data=f"""### Researchers\n**Total count:** {len(researchers)}\n\n**Most publications:** {", ".join(highest_researchers)}\n\n**Highest H-Index:** {'<a href="/researchers/' + str(highest_h_score["uuid"]) + '" target="_blank">' + highest_h_score["full_name"] + '</a> (' + str(highest_h_score["h_index"]) + ')' if highest_h_score is not None else "N/A"}""", entity_type=EntityType.RESEARCHER))
        result.add("institutions", Series(data=f"### Institutions\n**Total count:** {len(institutions)}", entity_type=EntityType.INSTITUTION))
        return result


class MixedInstitutionAggregation(Chart):
    identifier = "mixed_institution_aggregation"
    name = "Institution aggregation"
    type = ChartType.MIXED
    chart_template = ChartTemplates.DATATABLE
    generator = create_basic_generator(["institutions"])

    INSTITUTION_FIELD_NAME = "aggregate_field_name"

    @classmethod
    def to_table_series(cls, dataframe: pd.DataFrame, field_name: str) -> dict:
        headers = [str(c) for c in dataframe.columns]
        headers.insert(0, field_name)
        rows = [[index] + row for index, row in zip(dataframe.index, dataframe.values.tolist())]
        return {"header": headers, "rows": rows}

    async def get_series(self, chart_input: ChartInput) -> SeriesMap:
        result = SeriesMap()
        conditions = chart_input.get_series_conditions(Institution, "institutions")
        field_name = chart_input.special_fields.get(MixedInstitutionAggregation.INSTITUTION_FIELD_NAME)
        async with get_session() as session:
            institutions = (await session.execute(select(Institution).where(*conditions))).scalars().all()
        df = pd.DataFrame([i.model_dump() for i in institutions])
        df["avg_h_index"] = df["openalex_meta"].apply(lambda inst: inst.get("summary_stats", {}).get("h_index") if inst else None)
        grouped = df.groupby([field_name])
        count_series = grouped.count().rename(columns={"uuid": "count"})["count"]
        avg_h_index_series = grouped["avg_h_index"].mean()
        final_df = pd.concat([count_series, avg_h_index_series], axis=1)
        result.add("institutions", Series(data=MixedInstitutionAggregation.to_table_series(final_df, field_name), entity_type=EntityType.INSTITUTION))
        return result


class MixedWorkAggregation(Chart):
    identifier = "mixed_work_aggregation"
    name = "Work aggregation"
    type = ChartType.MIXED
    chart_template = ChartTemplates.DATATABLE
    generator = create_basic_generator(["works"])

    WORK_FIELD_NAME = "aggregate_field_name"

    async def get_series(self, chart_input: ChartInput) -> SeriesMap:
        result = SeriesMap()
        conditions = chart_input.get_series_conditions(Work, "works")
        field_name = chart_input.special_fields.get(MixedWorkAggregation.WORK_FIELD_NAME)
        async with get_session() as session:
            works = (await session.execute(select(Work).where(*conditions))).scalars().all()
        df = pd.DataFrame([w.model_dump() for w in works])
        df["avg_citations"] = df["openalex_meta"].apply(lambda m: int(m.get("cited_by_count", 0)) if m else None)
        df["dblp_type"] = df["type"].apply(lambda t: t.get("dblp") if t else None)
        df["openalex_type"] = df["type"].apply(lambda t: t.get("openalex") if t else None)
        grouped = df.groupby([field_name])
        count_series = grouped.count().rename(columns={"uuid": "count"})["count"]
        avg_citations_series = grouped["avg_citations"].mean()
        final_df = pd.concat([count_series, avg_citations_series], axis=1)
        result.add("works", Series(data=MixedInstitutionAggregation.to_table_series(final_df, field_name), entity_type=EntityType.WORK))
        return result


class MixedResearchActivity(Chart):
    identifier = "mixed_research_activity"
    name = "Research activity"
    type = ChartType.MIXED
    chart_template = ChartTemplates.ECHARTS
    generator = read_generator("mixedResearchActivity.js")

    @classmethod
    def get_chart_data(cls, works) -> dict:
        df = pd.DataFrame([w.model_dump() for w in works])
        grouped = df.groupby(["publication_date"])
        count_series = grouped.count().rename(columns={"uuid": "count"})
        return {"data": count_series["count"].tolist(), "date": count_series.index.tolist()}

    async def get_series(self, chart_input: ChartInput) -> SeriesMap:
        result = SeriesMap()
        conditions = chart_input.get_series_conditions(Work, "works")
        async with get_session() as session:
            works = (
                await session.execute(select(Work).where(Work.publication_date.isnot(None), *conditions))
            ).scalars().all()
        result.add("works", Series(data=MixedResearchActivity.get_chart_data(works), entity_type=EntityType.WORK))
        return result


class MixedActivityYearsTypes(Chart):
    identifier = "mixed_activity_years_types"
    name = "Publication activity (years/types)"
    type = ChartType.MIXED
    chart_template = ChartTemplates.ECHARTS
    generator = read_generator("researchActivityYearsTypes.js")

    TYPE_FIELD_NAME = "activity_field_name"

    async def get_series(self, chart_input: ChartInput) -> SeriesMap:
        result = SeriesMap()
        conditions = chart_input.get_series_conditions(Work, "works")
        field_name = chart_input.special_fields.get(MixedActivityYearsTypes.TYPE_FIELD_NAME)
        async with get_session() as session:
            works = (
                await session.execute(select(Work).where(Work.publication_year.isnot(None), *conditions))
            ).scalars().all()
        if works:
            df = pd.DataFrame([w.model_dump() for w in works])
            df["dblp_type"] = df["type"].apply(lambda t: t.get("dblp") if t else None)
            df["openalex_type"] = df["type"].apply(lambda t: t.get("openalex") if t else None)
            grouped = df.groupby(["publication_year", field_name]).size().unstack(fill_value=0)
            data = grouped.to_dict(orient="list")
            years = grouped.index.tolist()
        else:
            data, years = [], []
        result.add("works", Series(data={"data": data, "years": years}, entity_type=EntityType.WORK))
        return result


class KeywordCloud(Chart):
    identifier = "keyword_cloud"
    name = "Keywords (Cloud)"
    type = ChartType.MIXED
    chart_template = ChartTemplates.HIGHCHARTS
    generator = read_generator("keywordCloud.js")

    async def get_series(self, chart_input: ChartInput) -> SeriesMap:
        result = SeriesMap()
        conditions = chart_input.get_series_conditions(Work, "works")
        async with get_session() as session:
            works = (
                await session.execute(select(Work).where(Work.keywords.isnot(None), *conditions))
            ).scalars().all()
        df = pd.DataFrame([w.model_dump() for w in works])
        df = df.explode("keywords")
        grouped = df.groupby(["keywords"])
        count_df = grouped.count().rename(columns={"uuid": "weight"})
        data = count_df[["weight"]].to_dict(orient="index")
        result.add("works", Series(data=data, entity_type=EntityType.WORK))
        return result