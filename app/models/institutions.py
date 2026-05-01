from typing import Optional, Any
from uuid import UUID

from geoalchemy2 import Geometry
from geoalchemy2.shape import to_shape, from_shape
from pydantic import BaseModel
from shapely.geometry import Point
from sqlalchemy import Text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.models import EditableEntity, SNMEntityMixin


class InstitutionExternalId(BaseModel):
    grid: Optional[str] = None
    mag: Optional[str] = None
    openalex: Optional[str] = None
    ror: Optional[str] = None
    wikipedia: Optional[str] = None
    wikidata: Optional[str] = None

    def ror_match(self, external_id: "InstitutionExternalId") -> bool:
        return self.ror is not None and self.ror == external_id.ror

    def openalex_match(self, external_id: "InstitutionExternalId") -> bool:
        return self.openalex is not None and self.openalex == external_id.openalex

    def matches(self, external_id: "InstitutionExternalId") -> bool:
        return self.openalex_match(external_id) or self.ror_match(external_id)


class Institution(EditableEntity, SNMEntityMixin):
    __tablename__ = "institutions"

    # Embedded: InstitutionExternalId
    external_id_grid: Mapped[Optional[str]] = mapped_column(nullable=True, default=None)
    external_id_mag: Mapped[Optional[str]] = mapped_column(nullable=True, default=None)
    external_id_openalex: Mapped[Optional[str]] = mapped_column(nullable=True, default=None)
    external_id_ror: Mapped[Optional[str]] = mapped_column(nullable=True, default=None)
    external_id_wikipedia: Mapped[Optional[str]] = mapped_column(nullable=True, default=None)
    external_id_wikidata: Mapped[Optional[str]] = mapped_column(nullable=True, default=None)

    name: Mapped[str]
    acronyms: Mapped[list] = mapped_column(ARRAY(Text), default=list)
    alternative_names: Mapped[list] = mapped_column(ARRAY(Text), default=list)
    international_names: Mapped[dict] = mapped_column(JSONB, default=dict)
    city: Mapped[Optional[str]] = mapped_column(nullable=True, default=None)
    region: Mapped[Optional[str]] = mapped_column(nullable=True, default=None)
    country: Mapped[Optional[str]] = mapped_column(nullable=True, default=None)
    location: Mapped[Optional[Any]] = mapped_column(
        Geometry("POINT", srid=4326), nullable=True, default=None
    )
    homepage_url: Mapped[Optional[str]] = mapped_column(nullable=True, default=None)
    image_url: Mapped[Optional[str]] = mapped_column(nullable=True, default=None)
    parent_institutions_ids: Mapped[list] = mapped_column(ARRAY(Text), default=list)
    type: Mapped[Optional[str]] = mapped_column(nullable=True, default=None)
    topic_keywords: Mapped[list] = mapped_column(ARRAY(Text), default=list)
    openalex_meta: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True, default=None)
    orcid_meta: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True, default=None)
    dblp_meta: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True, default=None)

    researchers: Mapped[list["Researcher"]] = relationship(
        back_populates="institution", lazy="select"
    )
    affiliation_entries: Mapped[list["Affiliation"]] = relationship(
        back_populates="institution", lazy="select"
    )

    def __init__(self, *, external_id: Optional[InstitutionExternalId] = None, **kwargs):
        if external_id is not None:
            kwargs.update({
                "external_id_grid": external_id.grid,
                "external_id_mag": external_id.mag,
                "external_id_openalex": external_id.openalex,
                "external_id_ror": external_id.ror,
                "external_id_wikipedia": external_id.wikipedia,
                "external_id_wikidata": external_id.wikidata,
            })
        if "homepage_url" in kwargs and kwargs["homepage_url"] is not None:
            kwargs["homepage_url"] = str(kwargs["homepage_url"])
        if "image_url" in kwargs and kwargs["image_url"] is not None:
            kwargs["image_url"] = str(kwargs["image_url"])
        loc = kwargs.get("location")
        if loc is not None and isinstance(loc, tuple):
            lon, lat = loc
            kwargs["location"] = from_shape(Point(lon, lat), srid=4326)
        super().__init__(**kwargs)

    @property
    def external_id(self) -> InstitutionExternalId:
        return InstitutionExternalId(
            grid=self.external_id_grid,
            mag=self.external_id_mag,
            openalex=self.external_id_openalex,
            ror=self.external_id_ror,
            wikipedia=self.external_id_wikipedia,
            wikidata=self.external_id_wikidata,
        )

    @external_id.setter
    def external_id(self, value: InstitutionExternalId) -> None:
        self.external_id_grid = value.grid
        self.external_id_mag = value.mag
        self.external_id_openalex = value.openalex
        self.external_id_ror = value.ror
        self.external_id_wikipedia = value.wikipedia
        self.external_id_wikidata = value.wikidata

    @property
    def location_coords(self) -> Optional[tuple]:
        if self.location is None:
            return None
        point = to_shape(self.location)
        return point.x, point.y

    @property
    def normalized_name(self) -> str:
        return self.name.lower()

    def model_dump(self) -> dict:
        return {
            "id": self.id,
            "uuid": self.id,
            "imported_at": self.imported_at,
            "snm_key": self.snm_key,
            "duplication_key": self.duplication_key,
            "marked_for_removal": self.marked_for_removal,
            "external_id": self.external_id.model_dump(),
            "name": self.name,
            "acronyms": self.acronyms or [],
            "alternative_names": self.alternative_names or [],
            "international_names": self.international_names or {},
            "city": self.city,
            "region": self.region,
            "country": self.country,
            "location": self.location_coords,
            "homepage_url": self.homepage_url,
            "image_url": self.image_url,
            "parent_institutions_ids": self.parent_institutions_ids or [],
            "type": self.type,
            "topic_keywords": self.topic_keywords or [],
            "openalex_meta": self.openalex_meta,
            "orcid_meta": self.orcid_meta,
            "dblp_meta": self.dblp_meta,
        }