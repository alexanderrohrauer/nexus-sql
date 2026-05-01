import logging

import pydash as _
from aiohttp_client_cache import CachedSession, SQLiteBackend

from app.settings import get_settings
from app.test_data import TestDataInjector
from app.utils.text_utils import parse_openalex_id

settings = get_settings()

OPENALEX_URL = "https://api.openalex.org"
OPENALEX_AUTHOR_BATCH_SIZE = settings.openalex_batch_size
OPENALEX_INSTITUTION_BATCH_SIZE = settings.openalex_batch_size

logger = logging.getLogger("uvicorn.error")

works_test_data = TestDataInjector()
authors_test_data = TestDataInjector()
institutions_test_data = TestDataInjector()


class OpenAlexSession(CachedSession):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, headers={"User-Agent": "mailto:k12105578@students.jku.at"},
                         cache=SQLiteBackend('openalex_cache', expire_after=-1))


async def fetch_topics(keywords: list[str]):
    result = []
    for keyword in keywords:
        params = {"filter": f"default.search:{keyword}", "per-page": 1}
        async with OpenAlexSession() as session:
            logger.debug("Fetching topics...")
            response = await session.get(f"{OPENALEX_URL}/topics", params=params)
            body = await response.json()
            if len(body["results"]) > 0:
                topic = body["results"][0]
                logger.info(f"Adding topic {topic['display_name']}")
                result.append(topic)
            else:
                result.append(keyword)
    return result


async def fetch_works(topic_ids_or_kws: list[dict | str], page: int, page_size: int):
    result = []
    topic_ids = []
    keywords = []

    for topic_or_kw in topic_ids_or_kws:
        if isinstance(topic_or_kw, str):
            keywords.append(topic_or_kw)
        else:
            topic_ids.append(parse_openalex_id(topic_or_kw["id"]))

    if len(topic_ids) > 0:
        topic_expr = "|".join(topic_ids)
        params = {"filter": f"topics.id:{topic_expr}", "page": page, "per-page": page_size,
                  "sort": "publication_year:desc"}
        async with OpenAlexSession() as session:
            response = await session.get(f"{OPENALEX_URL}/works", params=params)
            logger.debug(f"Fetching topic works ({response.url})...")
            body = await response.json()
            result = body["results"]

    for keyword in keywords:
        params = {"filter": f"title_and_abstract.search:{keyword}", "page": page, "per-page": page_size,
                  "sort": "publication_year:desc"}
        async with OpenAlexSession() as session:
            response = await session.get(f"{OPENALEX_URL}/works", params=params)
            logger.debug(f"Fetching keyword works ({response.url})...")
            body = await response.json()
            result = result + body["results"]
    works_test_data.inject(page - 1, "openalex_works.json", result)
    return result


async def fetch_authors_for_works(works: list[dict], batch_id: int) -> list[dict]:
    authors = []
    author_ids = _.uniq(_.flatten(
        map(lambda w: [parse_openalex_id(a["author"]["id"]) for a in w["authorships"]],
            works)))
    author_id_chunks = _.chunk(author_ids, OPENALEX_AUTHOR_BATCH_SIZE)
    for chunk in author_id_chunks:
        chunk = list(filter(lambda c: c is not None, chunk))
        chunk_expr = "|".join(chunk)
        params = {"filter": f"ids.openalex:{chunk_expr}", "per-page": OPENALEX_AUTHOR_BATCH_SIZE}
        async with OpenAlexSession() as session:
            response = await session.get(f"{OPENALEX_URL}/authors", params=params)
            logger.debug(f"Fetching authors ({response.url})...")
            body = await response.json()
        authors = authors + body["results"]
    authors_test_data.inject(batch_id, "openalex_authors.json", authors)
    return authors


async def fetch_institutions_for_authors(authors: list[dict], batch_id: int) -> list[dict]:
    institutions = []
    institution_ids = _.uniq(_.flatten(
        map(lambda a: [parse_openalex_id(a["institution"]["id"]) for a in a["affiliations"]],
            authors)))
    last_institution_authors = filter(lambda a: len(a["last_known_institutions"]) > 0 if a["last_known_institutions"] is not None else [], authors)
    institution_ids = institution_ids + [parse_openalex_id(a["last_known_institutions"][0]["id"]) for a in last_institution_authors]
    institution_id_chunks = _.chunk(institution_ids, OPENALEX_INSTITUTION_BATCH_SIZE)
    for chunk in institution_id_chunks:
        chunk_expr = "|".join(chunk)
        params = {"filter": f"ids.openalex:{chunk_expr}", "per-page": OPENALEX_AUTHOR_BATCH_SIZE}
        async with OpenAlexSession() as session:
            response = await session.get(f"{OPENALEX_URL}/institutions", params=params)
            logger.debug(f"Fetching institutions ({response.url})...")
            body = await response.json()
        institutions = institutions + body["results"]
    institutions_test_data.inject(batch_id, "openalex_institutions.json", institutions)
    return institutions
