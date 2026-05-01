import json
import re
from abc import abstractmethod
from datetime import datetime
from typing import Optional, Any
from uuid import UUID

from pydantic import BaseModel
from sqlalchemy import asc, desc
from sqlalchemy.orm import InstrumentedAttribute


def _get_column(model_class, field_path: str):
    """Maps a (possibly dotted) field path to a SQLAlchemy column attribute."""
    if "." in field_path:
        prefix, attr = field_path.split(".", 1)
        col_name = f"{prefix}_{attr}"
        return getattr(model_class, col_name, None)
    if field_path == "uuid":
        return model_class.id
    return getattr(model_class, field_path, None)


def transform_filter_field(field: dict):
    if field["field"].endswith("imported_at") or field["field"].endswith("publication_date"):
        return datetime.fromisoformat(field["value"]) if field["value"] is not None else None
    if field["operator"] == "$regex":
        return re.compile(field["value"], flags=re.IGNORECASE) if field["value"] is not None else None
    if field["field"].endswith("uuid") or field["field"].endswith(".id"):
        return [UUID(opt["value"]) for opt in field["value"] if opt["value"] is not None]
    if field["field"].endswith("duplication_key"):
        return UUID(field["value"]) if field["value"] is not None else None
    return field["value"]


def build_conditions(model_class, criteria: list) -> list:
    """Converts [{field, operator, value}] criteria to SQLAlchemy conditions."""
    conditions = []
    for criterion in criteria:
        column = _get_column(model_class, criterion["field"])
        if column is None:
            continue
        operator = criterion["operator"]
        value = transform_filter_field(criterion)
        if operator == "$regex":
            pattern = value.pattern if hasattr(value, "pattern") else str(value)
            conditions.append(column.ilike(f"%{pattern}%"))
        elif operator in ("$eq", "="):
            conditions.append(column == value)
        elif operator == "$ne":
            conditions.append(column != value)
        elif operator == "$gt":
            conditions.append(column > value)
        elif operator == "$gte":
            conditions.append(column >= value)
        elif operator == "$lt":
            conditions.append(column < value)
        elif operator == "$lte":
            conditions.append(column <= value)
        elif operator == "$in":
            conditions.append(column.in_(value))
        elif operator == "$nin":
            conditions.append(column.notin_(value))
        else:
            conditions.append(column == value)
    return conditions


class SearchAndFilterParams:
    def __init__(
        self,
        search: Optional[str] = None,
        q: str = "[]",
        sort: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
    ):
        self.q = q
        self.sort = sort
        self.search = search
        self.limit = limit
        self.offset = offset

    @abstractmethod
    def get_search_conditions(self, model_class) -> list:
        raise NotImplementedError("get_search_conditions() not implemented")

    def get_criteria(self) -> list:
        return json.loads(self.q)

    def get_conditions(self, model_class) -> list:
        criteria = self.get_criteria()
        conditions = build_conditions(model_class, criteria)
        if self.search is not None:
            conditions.extend(self.get_search_conditions(model_class))
        return conditions

    def get_order_by(self, model_class) -> list:
        result = []
        if self.sort is not None:
            for sort in self.sort.split(","):
                sort = sort.strip()
                if not sort:
                    continue
                if sort.startswith("+") or sort.startswith("-"):
                    prefix, field = sort[0], sort[1:]
                    col = _get_column(model_class, field)
                    if col is not None:
                        result.append(asc(col) if prefix == "+" else desc(col))
                else:
                    col = _get_column(model_class, sort)
                    if col is not None:
                        result.append(asc(col))
        return result


class ResponseModel(BaseModel):
    @classmethod
    def from_model(cls, model: Any):
        raise NotImplementedError()

    @classmethod
    def from_model_list(cls, models: list):
        return [cls.from_model(m) for m in models]