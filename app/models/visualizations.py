from typing import Optional
from uuid import UUID, uuid4

from pydantic import Field, BaseModel
from sqlalchemy import Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.models import BaseEntity


class Visualization(BaseModel):
    uuid: str = Field(default_factory=lambda: str(uuid4()))
    title: str = Field(min_length=1)
    rows: int = Field(gt=1)
    columns: int = Field(gt=1)
    chart: str = Field(min_length=1)
    query_preset: dict = Field(default={})
    special_fields: dict = Field(default={})


class Dashboard(BaseEntity):
    __tablename__ = "dashboards"

    title: Mapped[str] = mapped_column(Text)
    visualizations_json: Mapped[list] = mapped_column(
        JSONB, default=list, name="visualizations"
    )

    def __init__(self, *, title: str, visualizations: Optional[list] = None, **kwargs):
        vis_data = [
            v.model_dump() if isinstance(v, Visualization) else v
            for v in (visualizations or [])
        ]
        super().__init__(title=title, visualizations_json=vis_data, **kwargs)

    @property
    def visualizations(self) -> list[Visualization]:
        return [Visualization(**v) for v in (self.visualizations_json or [])]

    @visualizations.setter
    def visualizations(self, value: list[Visualization]) -> None:
        self.visualizations_json = [
            v.model_dump() if isinstance(v, Visualization) else v for v in value
        ]

    def model_dump(self) -> dict:
        return {
            "id": self.id,
            "uuid": self.id,
            "title": self.title,
            "visualizations": [v.model_dump() for v in self.visualizations],
        }