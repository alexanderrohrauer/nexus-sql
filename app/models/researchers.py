from enum import Enum
from typing import Optional
from uuid import UUID

from pydantic import BaseModel
from sqlalchemy import Text, Integer, ForeignKey
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.models import EditableEntity, SNMEntityMixin


class ResearcherExternalId(BaseModel):
    openalex: Optional[str] = None
    orcid: Optional[str] = None
    dblp: Optional[str] = None
    scopus: Optional[str] = None
    twitter: Optional[str] = None
    wikipedia: Optional[str] = None

    def openalex_match(self, external_id: "ResearcherExternalId") -> bool:
        return self.openalex is not None and external_id.openalex == self.openalex

    def dblp_match(self, external_id: "ResearcherExternalId") -> bool:
        return self.dblp is not None and external_id.dblp == self.dblp

    def orcid_match(self, external_id: "ResearcherExternalId") -> bool:
        return self.orcid is not None and external_id.orcid == self.orcid

    def matches(self, external_id: "ResearcherExternalId") -> bool:
        return (self.openalex_match(external_id) or
                self.dblp_match(external_id) or
                self.orcid_match(external_id))


class AffiliationType(Enum):
    EDUCATION = "EDUCATION"
    EMPLOYMENT = "EMPLOYMENT"


class Affiliation(EditableEntity):
    __tablename__ = "affiliations"

    researcher_id: Mapped[UUID] = mapped_column(
        ForeignKey("researchers.id", ondelete="CASCADE"), nullable=False
    )
    institution_id: Mapped[UUID] = mapped_column(
        ForeignKey("institutions.id", ondelete="CASCADE"), nullable=False
    )
    years: Mapped[list] = mapped_column(ARRAY(Integer), default=list)
    affiliation_type: Mapped[Optional[str]] = mapped_column(nullable=True, default=None)

    researcher: Mapped["Researcher"] = relationship(back_populates="affiliations")
    institution: Mapped["Institution"] = relationship(back_populates="affiliation_entries")

    def __init__(self, *, institution=None, type: Optional[AffiliationType] = None, **kwargs):
        if institution is not None:
            kwargs["institution_id"] = institution if isinstance(institution, UUID) else institution.id
        if type is not None:
            kwargs["affiliation_type"] = type.value if isinstance(type, AffiliationType) else type
        super().__init__(**kwargs)

    @property
    def type(self) -> Optional[AffiliationType]:
        return AffiliationType(self.affiliation_type) if self.affiliation_type else None

    @type.setter
    def type(self, value: Optional[AffiliationType]) -> None:
        self.affiliation_type = value.value if value is not None else None

    def model_dump(self) -> dict:
        institution_data = None
        try:
            if self.institution is not None:
                institution_data = self.institution.model_dump()
        except Exception:
            institution_data = {"id": self.institution_id} if self.institution_id else None
        return {
            "id": self.id,
            "uuid": self.id,
            "imported_at": self.imported_at,
            "years": self.years or [],
            "type": self.affiliation_type,
            "institution": institution_data,
            "researcher_id": self.researcher_id,
        }


class Researcher(EditableEntity, SNMEntityMixin):
    __tablename__ = "researchers"

    # Embedded: ResearcherExternalId
    external_id_openalex: Mapped[Optional[str]] = mapped_column(nullable=True, default=None)
    external_id_orcid: Mapped[Optional[str]] = mapped_column(nullable=True, default=None)
    external_id_dblp: Mapped[Optional[str]] = mapped_column(nullable=True, default=None)
    external_id_scopus: Mapped[Optional[str]] = mapped_column(nullable=True, default=None)
    external_id_twitter: Mapped[Optional[str]] = mapped_column(nullable=True, default=None)
    external_id_wikipedia: Mapped[Optional[str]] = mapped_column(nullable=True, default=None)

    full_name: Mapped[str]
    alternative_names: Mapped[Optional[list]] = mapped_column(ARRAY(Text), nullable=True, default=None)
    institution_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey("institutions.id", ondelete="SET NULL"), nullable=True, default=None
    )
    topic_keywords: Mapped[Optional[list]] = mapped_column(ARRAY(Text), nullable=True, default=None)
    openalex_meta: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True, default=None)
    orcid_meta: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True, default=None)
    dblp_meta: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True, default=None)

    institution: Mapped[Optional["Institution"]] = relationship(
        back_populates="researchers", lazy="select"
    )
    affiliations: Mapped[list["Affiliation"]] = relationship(
        back_populates="researcher", lazy="select", cascade="all, delete-orphan"
    )
    works: Mapped[list["Work"]] = relationship(
        secondary="work_authors", back_populates="authors", lazy="select"
    )

    def __init__(
        self,
        *,
        external_id: Optional[ResearcherExternalId] = None,
        institution=None,
        affiliations: Optional[list] = None,
        **kwargs,
    ):
        if external_id is not None:
            kwargs.update({
                "external_id_openalex": external_id.openalex,
                "external_id_orcid": external_id.orcid,
                "external_id_dblp": external_id.dblp,
                "external_id_scopus": external_id.scopus,
                "external_id_twitter": external_id.twitter,
                "external_id_wikipedia": external_id.wikipedia,
            })
        if institution is not None:
            kwargs["institution_id"] = institution if isinstance(institution, UUID) else institution.id
        super().__init__(**kwargs)
        if affiliations is not None:
            self.affiliations = affiliations

    @property
    def external_id(self) -> ResearcherExternalId:
        return ResearcherExternalId(
            openalex=self.external_id_openalex,
            orcid=self.external_id_orcid,
            dblp=self.external_id_dblp,
            scopus=self.external_id_scopus,
            twitter=self.external_id_twitter,
            wikipedia=self.external_id_wikipedia,
        )

    @external_id.setter
    def external_id(self, value: ResearcherExternalId) -> None:
        self.external_id_openalex = value.openalex
        self.external_id_orcid = value.orcid
        self.external_id_dblp = value.dblp
        self.external_id_scopus = value.scopus
        self.external_id_twitter = value.twitter
        self.external_id_wikipedia = value.wikipedia

    @property
    def normalized_full_name(self) -> str:
        return self.full_name.lower()

    def model_dump(self) -> dict:
        institution_data = None
        try:
            if self.institution is not None:
                institution_data = self.institution.model_dump()
        except Exception:
            institution_data = {"id": self.institution_id} if self.institution_id else None

        affiliations_data = []
        try:
            affiliations_data = [a.model_dump() for a in (self.affiliations or [])]
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
            "full_name": self.full_name,
            "alternative_names": self.alternative_names,
            "affiliations": affiliations_data,
            "institution": institution_data,
            "topic_keywords": self.topic_keywords,
            "openalex_meta": self.openalex_meta,
            "orcid_meta": self.orcid_meta,
            "dblp_meta": self.dblp_meta,
        }