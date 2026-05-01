from datetime import date
from typing import Optional
from uuid import UUID

from pydantic import BaseModel
from sqlalchemy import Text, Date, ForeignKey, Column, Table
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.models import EditableEntity, SNMEntityMixin, Base

work_authors = Table(
    "work_authors",
    Base.metadata,
    Column("work_id", ForeignKey("works.id", ondelete="CASCADE"), primary_key=True),
    Column("researcher_id", ForeignKey("researchers.id", ondelete="CASCADE"), primary_key=True),
)


class WorkExternalId(BaseModel):
    openalex: Optional[str] = None
    mag: Optional[str] = None
    dblp: Optional[str] = None
    doi: Optional[str] = None
    pmid: Optional[str] = None
    pmcid: Optional[str] = None

    def openalex_match(self, external_id: "WorkExternalId") -> bool:
        return self.openalex is not None and self.openalex == external_id.openalex

    def dblp_match(self, external_id: "WorkExternalId") -> bool:
        return self.dblp is not None and self.dblp == external_id.dblp

    def doi_match(self, external_id: "WorkExternalId") -> bool:
        return self.doi is not None and self.doi == external_id.doi

    def matches(self, external_id: "WorkExternalId") -> bool:
        return (self.openalex_match(external_id) or
                self.dblp_match(external_id) or
                self.doi_match(external_id))


class WorkType(BaseModel):
    openalex: Optional[str] = None
    orcid: Optional[str] = None
    dblp: Optional[str] = None


class Work(EditableEntity, SNMEntityMixin):
    __tablename__ = "works"

    # Embedded: WorkExternalId
    external_id_openalex: Mapped[Optional[str]] = mapped_column(nullable=True, default=None)
    external_id_mag: Mapped[Optional[str]] = mapped_column(nullable=True, default=None)
    external_id_dblp: Mapped[Optional[str]] = mapped_column(nullable=True, default=None)
    external_id_doi: Mapped[Optional[str]] = mapped_column(nullable=True, default=None)
    external_id_pmid: Mapped[Optional[str]] = mapped_column(nullable=True, default=None)
    external_id_pmcid: Mapped[Optional[str]] = mapped_column(nullable=True, default=None)

    # Embedded: WorkType
    type_openalex: Mapped[Optional[str]] = mapped_column(nullable=True, default=None)
    type_orcid: Mapped[Optional[str]] = mapped_column(nullable=True, default=None)
    type_dblp: Mapped[Optional[str]] = mapped_column(nullable=True, default=None)

    title: Mapped[str]
    publication_year: Mapped[int]
    publication_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True, default=None)
    keywords: Mapped[Optional[list]] = mapped_column(ARRAY(Text), nullable=True, default=None)
    language: Mapped[Optional[str]] = mapped_column(nullable=True, default=None)
    open_access: Mapped[Optional[bool]] = mapped_column(nullable=True, default=None)
    openalex_meta: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True, default=None)
    orcid_meta: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True, default=None)
    dblp_meta: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True, default=None)

    authors: Mapped[list["Researcher"]] = relationship(
        secondary=work_authors, back_populates="works", lazy="select"
    )

    def __init__(
        self,
        *,
        external_id: Optional[WorkExternalId] = None,
        type: Optional[WorkType] = None,
        authors: Optional[list] = None,
        **kwargs,
    ):
        if external_id is not None:
            kwargs.update({
                "external_id_openalex": external_id.openalex,
                "external_id_mag": external_id.mag,
                "external_id_dblp": external_id.dblp,
                "external_id_doi": external_id.doi,
                "external_id_pmid": external_id.pmid,
                "external_id_pmcid": external_id.pmcid,
            })
        if type is not None:
            kwargs.update({
                "type_openalex": type.openalex,
                "type_orcid": type.orcid,
                "type_dblp": type.dblp,
            })
        super().__init__(**kwargs)
        if authors is not None:
            self.authors = authors

    @property
    def external_id(self) -> WorkExternalId:
        return WorkExternalId(
            openalex=self.external_id_openalex,
            mag=self.external_id_mag,
            dblp=self.external_id_dblp,
            doi=self.external_id_doi,
            pmid=self.external_id_pmid,
            pmcid=self.external_id_pmcid,
        )

    @external_id.setter
    def external_id(self, value: WorkExternalId) -> None:
        self.external_id_openalex = value.openalex
        self.external_id_mag = value.mag
        self.external_id_dblp = value.dblp
        self.external_id_doi = value.doi
        self.external_id_pmid = value.pmid
        self.external_id_pmcid = value.pmcid

    @property
    def type(self) -> WorkType:
        return WorkType(
            openalex=self.type_openalex,
            orcid=self.type_orcid,
            dblp=self.type_dblp,
        )

    @type.setter
    def type(self, value: WorkType) -> None:
        self.type_openalex = value.openalex
        self.type_orcid = value.orcid
        self.type_dblp = value.dblp

    @property
    def normalized_title(self) -> str:
        return self.title.lower()

    def replace_author(self, researcher: "Researcher", replacement: "Researcher") -> None:
        if self.authors is not None:
            try:
                idx = next(i for i, a in enumerate(self.authors) if a.id == researcher.id)
                self.authors[idx] = replacement
            except StopIteration:
                raise Exception(f"Researcher {researcher.id} not found in work {self.id}")

    def model_dump(self) -> dict:
        authors_data = []
        try:
            authors_data = [a.model_dump() for a in (self.authors or [])]
        except Exception:
            pass
        return {
            "id": self.id,
            "uuid": self.id,
            "imported_at": self.imported_at,
            "snm_key": self.snm_key,
            "duplication_key": self.duplication_key,
            "marked_for_removal": self.marked_for_removal,
            "external_id": self.external_id.model_dump(),
            "title": self.title,
            "type": self.type.model_dump(),
            "publication_year": self.publication_year,
            "publication_date": self.publication_date,
            "keywords": self.keywords,
            "authors": authors_data,
            "language": self.language,
            "open_access": self.open_access,
            "openalex_meta": self.openalex_meta,
            "orcid_meta": self.orcid_meta,
            "dblp_meta": self.dblp_meta,
        }