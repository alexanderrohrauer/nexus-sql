from abc import abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Any

import importlib_resources
import pydash as _
from pydantic import BaseModel

from app.models import Researcher, Work, Institution
from app.utils.api_utils import build_conditions


class ChartTemplates:
    ECHARTS = "ECHARTS"
    HIGHCHARTS = "HIGHCHARTS"
    LEAFLET = "LEAFLET"
    DATATABLE = "DATATABLE"
    MARKDOWN = "MARKDOWN"
    CUSTOM = "CUSTOM"


class ChartType(Enum):
    MIXED = "MIXED"
    RESEARCHER = "RESEARCHER"
    WORK = "WORK"
    INSTITUTION = "INSTITUTION"


class EntityType(Enum):
    RESEARCHER = "RESEARCHER"
    WORK = "WORK"
    INSTITUTION = "INSTITUTION"
    AFFILIATIONS = "AFFILIATIONS"


class Series(BaseModel):
    data: Any
    entity_type: Optional[EntityType] = None


class SeriesMap(BaseModel):
    data: dict[str, Series] = {}

    def add(self, identifier: str, series: Series):
        self.data[identifier] = series

    def __add__(self, other):
        if isinstance(other, SeriesMap):
            self.data = self.data | other.data
            return self


def read_generator(filename: str) -> str:
    with importlib_resources.path("app.resources.charts", filename) as file:
        return open(file, "r").read()


def create_basic_generator(series_names: list[str]) -> str:
    series = ['nexus.series("' + name + '")' for name in series_names]
    joined = ",\n".join(series)
    return f"""
        export default function(nexus) {{
            return {{
                series: [
                    {joined}
                ]
            }}
        }}
    """


@dataclass
class ChartInput:
    queries: dict
    pre_filters: dict
    special_fields: dict
    work: Optional[Work] = None
    researcher: Optional[Researcher] = None
    institution: Optional[Institution] = None

    def get_series_criteria(self, series: str) -> list:
        """Returns the raw criteria list for a given series key."""
        query = self.queries.get(series, [])
        pre_filter = self.pre_filters.get(series, [])
        return query + pre_filter

    def get_series_conditions(self, model_class, series: str) -> list:
        """Returns SQLAlchemy conditions for the given series."""
        criteria = self.get_series_criteria(series)
        return build_conditions(model_class, criteria)

    def get_all_queries(self):
        return _.merge(self.queries, self.pre_filters)


class Chart:
    identifier: str
    name: str
    type: ChartType
    chart_template: str
    generator: str

    @abstractmethod
    async def get_series(self, chart_input: ChartInput) -> SeriesMap:
        raise NotImplementedError("get_series() not implemented")