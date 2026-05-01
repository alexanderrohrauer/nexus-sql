import html
import logging
from datetime import date
from string import digits

from app.models import Work, Researcher, Institution
from app.models.institutions import InstitutionExternalId
from app.models.researchers import ResearcherExternalId, Affiliation, AffiliationType
from app.models.works import WorkExternalId, WorkType
from app.utils.text_utils import (
    parse_openalex_id, parse_doi, parse_orcid, parse_ror,
    compute_work_snm_key, compute_researcher_snm_key, compute_institution_snm_key,
)

logger = logging.getLogger("uvicorn.error")


def restructure_works(works: list[dict], authors: list[Researcher]) -> list[Work]:
    result = []
    for work in works:
        if work["title"] is not None:
            keywords = [kw["display_name"] for kw in work["keywords"]]
            author_objects = []
            for authorship in work["authorships"]:
                author_id = parse_openalex_id(authorship["author"]["id"])
                try:
                    author_objects.append(
                        next(a for a in authors if a.external_id_openalex == author_id)
                    )
                except StopIteration:
                    logger.error(f"Author {author_id} not found in work {work['id']}")
                    continue
            ids = work["ids"].copy()
            ids["openalex"] = parse_openalex_id(ids["openalex"])
            ids["doi"] = parse_doi(ids["doi"]) if "doi" in ids else None
            parsed = Work(
                external_id=WorkExternalId(**ids),
                title=html.unescape(work["title"].strip()),
                type=WorkType(openalex=work["type"]),
                publication_year=int(work["publication_year"]),
                publication_date=date.fromisoformat(work["publication_date"]),
                keywords=keywords,
                authors=author_objects,
                language=work["language"],
                open_access=work["open_access"]["is_oa"],
                openalex_meta=work,
            )
            parsed.snm_key = compute_work_snm_key(parsed)
            result.append(parsed)
    return result


def restructure_authors(authors: list[dict], institutions: list[Institution]) -> list[Researcher]:
    result = []
    for author in authors:
        institution = None
        affiliations = []
        for affiliation in author["affiliations"]:
            institution_id = parse_openalex_id(affiliation["institution"]["id"])
            try:
                inst = next(i for i in institutions if i.external_id_openalex == institution_id)
                i_type = AffiliationType.EDUCATION if inst.type == "Education" else AffiliationType.EMPLOYMENT
                affiliations.append(
                    Affiliation(years=affiliation["years"], type=i_type, institution=inst.id)
                )
                institution = inst
            except StopIteration:
                logger.error(f"Institution {institution_id} not found for author {author['id']}")
                continue
        if author["last_known_institutions"] is not None and len(author["last_known_institutions"]) > 0:
            institution_id = parse_openalex_id(author["last_known_institutions"][0]["id"])
            try:
                institution = next(i for i in institutions if i.external_id_openalex == institution_id)
            except StopIteration:
                pass
        ids = author["ids"].copy()
        ids["openalex"] = parse_openalex_id(ids["openalex"])
        ids["orcid"] = parse_orcid(ids["orcid"]) if "orcid" in ids else None
        parsed = Researcher(
            external_id=ResearcherExternalId(**ids),
            full_name=author["display_name"].strip().translate(str.maketrans("", "", digits)),
            alternative_names=author["display_name_alternatives"],
            affiliations=affiliations,
            institution=institution.id if institution is not None else None,
            topic_keywords=[t["display_name"] for t in author["topics"]],
            openalex_meta=author,
        )
        parsed.snm_key = compute_researcher_snm_key(parsed)
        result.append(parsed)
    return result


def restructure_institutions(institutions: list[dict]) -> list[Institution]:
    result = []
    for institution in institutions:
        ids = institution["ids"].copy()
        ids["openalex"] = parse_openalex_id(ids["openalex"])
        ids["ror"] = parse_ror(ids["ror"]) if "ror" in ids else None
        geo = institution["geo"]
        location = (geo["longitude"], geo["latitude"]) if geo.get("longitude") else None
        parsed = Institution(
            external_id=InstitutionExternalId(**ids),
            name=institution["display_name"].strip(),
            acronyms=institution["display_name_acronyms"],
            alternative_names=institution["display_name_alternatives"],
            international_names=institution["international"]["display_name"] if "display_name" in institution["international"] else {},
            city=geo["city"],
            region=geo["region"],
            country=geo["country_code"],
            location=location,
            homepage_url=institution["homepage_url"],
            image_url=institution["image_url"],
            parent_institutions_ids=[parse_openalex_id(url) for url in institution["lineage"]],
            type=institution["type"],
            topic_keywords=[t["display_name"] for t in institution["topics"]],
            openalex_meta=institution,
        )
        parsed.snm_key = compute_institution_snm_key(parsed)
        result.append(parsed)
    return result