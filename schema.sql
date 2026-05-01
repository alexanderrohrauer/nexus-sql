-- nexus-sql PostgreSQL schema with PostGIS
-- Requires: CREATE EXTENSION postgis;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS postgis;

-- ─────────────────────────────────────────────────────────────────────────────
-- institutions
-- InstitutionExternalId is embedded as flat columns (JPA @Embeddable style)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE institutions (
    -- identity
    id                      UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    imported_at             TIMESTAMPTZ     NOT NULL DEFAULT now(),

    -- SNM (sliding-window deduplication)
    snm_key                 TEXT,
    duplication_key         UUID,
    marked_for_removal      BOOLEAN         NOT NULL DEFAULT FALSE,

    -- embedded: InstitutionExternalId
    external_id_grid        TEXT,
    external_id_mag         TEXT,
    external_id_openalex    TEXT,
    external_id_ror         TEXT,
    external_id_wikipedia   TEXT,
    external_id_wikidata    TEXT,

    -- institution fields
    name                    TEXT            NOT NULL,
    acronyms                TEXT[]          NOT NULL DEFAULT '{}',
    alternative_names       TEXT[]          NOT NULL DEFAULT '{}',
    international_names     JSONB           NOT NULL DEFAULT '{}',
    city                    TEXT,
    region                  TEXT,
    country                 CHAR(2),
    location                GEOMETRY(POINT, 4326),   -- longitude / latitude
    homepage_url            TEXT,
    image_url               TEXT,
    parent_institutions_ids TEXT[]          NOT NULL DEFAULT '{}',
    type                    TEXT,
    topic_keywords          TEXT[]          NOT NULL DEFAULT '{}',

    -- raw API payloads
    openalex_meta           JSONB,
    orcid_meta              JSONB,
    dblp_meta               JSONB
);

CREATE INDEX idx_institutions_snm_key         ON institutions (snm_key);
CREATE INDEX idx_institutions_duplication_key ON institutions (duplication_key);
CREATE INDEX idx_institutions_external_openalex ON institutions (external_id_openalex);
CREATE INDEX idx_institutions_external_ror    ON institutions (external_id_ror);
CREATE INDEX idx_institutions_location        ON institutions USING GIST (location);


-- ─────────────────────────────────────────────────────────────────────────────
-- researchers
-- ResearcherExternalId is embedded as flat columns
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE researchers (
    -- identity
    id                      UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    imported_at             TIMESTAMPTZ     NOT NULL DEFAULT now(),

    -- SNM
    snm_key                 TEXT,
    duplication_key         UUID,
    marked_for_removal      BOOLEAN         NOT NULL DEFAULT FALSE,

    -- embedded: ResearcherExternalId
    external_id_openalex    TEXT,
    external_id_orcid       TEXT,
    external_id_dblp        TEXT,
    external_id_scopus      TEXT,
    external_id_twitter     TEXT,
    external_id_wikipedia   TEXT,

    -- researcher fields
    full_name               TEXT            NOT NULL,
    alternative_names       TEXT[],
    institution_id          UUID            REFERENCES institutions (id) ON DELETE SET NULL,
    topic_keywords          TEXT[],

    -- raw API payloads
    openalex_meta           JSONB,
    orcid_meta              JSONB,
    dblp_meta               JSONB
);

CREATE INDEX idx_researchers_snm_key          ON researchers (snm_key);
CREATE INDEX idx_researchers_duplication_key  ON researchers (duplication_key);
CREATE INDEX idx_researchers_institution_id   ON researchers (institution_id);
CREATE INDEX idx_researchers_external_openalex ON researchers (external_id_openalex);
CREATE INDEX idx_researchers_external_dblp    ON researchers (external_id_dblp);
CREATE INDEX idx_researchers_external_orcid   ON researchers (external_id_orcid);


-- ─────────────────────────────────────────────────────────────────────────────
-- affiliations  (one researcher → many affiliations → one institution each)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE affiliations (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    imported_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    researcher_id   UUID        NOT NULL REFERENCES researchers (id) ON DELETE CASCADE,
    institution_id  UUID        NOT NULL REFERENCES institutions (id) ON DELETE CASCADE,
    years           INTEGER[]   NOT NULL DEFAULT '{}',
    affiliation_type TEXT                           -- 'EDUCATION' | 'EMPLOYMENT'
);

CREATE INDEX idx_affiliations_researcher_id ON affiliations (researcher_id);
CREATE INDEX idx_affiliations_institution_id ON affiliations (institution_id);


-- ─────────────────────────────────────────────────────────────────────────────
-- works
-- WorkExternalId and WorkType are each embedded as flat columns
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE works (
    -- identity
    id                      UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    imported_at             TIMESTAMPTZ     NOT NULL DEFAULT now(),

    -- SNM
    snm_key                 TEXT,
    duplication_key         UUID,
    marked_for_removal      BOOLEAN         NOT NULL DEFAULT FALSE,

    -- embedded: WorkExternalId
    external_id_openalex    TEXT,
    external_id_mag         TEXT,
    external_id_dblp        TEXT,
    external_id_doi         TEXT,
    external_id_pmid        TEXT,
    external_id_pmcid       TEXT,

    -- embedded: WorkType
    type_openalex           TEXT,
    type_orcid              TEXT,
    type_dblp               TEXT,

    -- work fields
    title                   TEXT            NOT NULL,
    publication_year        INTEGER         NOT NULL,
    publication_date        DATE,
    keywords                TEXT[],
    language                CHAR(3),
    open_access             BOOLEAN,

    -- raw API payloads
    openalex_meta           JSONB,
    orcid_meta              JSONB,
    dblp_meta               JSONB
);

CREATE INDEX idx_works_snm_key            ON works (snm_key);
CREATE INDEX idx_works_duplication_key    ON works (duplication_key);
CREATE INDEX idx_works_publication_year   ON works (publication_year);
CREATE INDEX idx_works_external_openalex  ON works (external_id_openalex);
CREATE INDEX idx_works_external_dblp      ON works (external_id_dblp);
CREATE INDEX idx_works_external_doi       ON works (external_id_doi);


-- ─────────────────────────────────────────────────────────────────────────────
-- work_authors  (many-to-many: works ↔ researchers)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE work_authors (
    work_id         UUID NOT NULL REFERENCES works (id) ON DELETE CASCADE,
    researcher_id   UUID NOT NULL REFERENCES researchers (id) ON DELETE CASCADE,
    PRIMARY KEY (work_id, researcher_id)
);

CREATE INDEX idx_work_authors_work_id       ON work_authors (work_id);
CREATE INDEX idx_work_authors_researcher_id ON work_authors (researcher_id);


-- ─────────────────────────────────────────────────────────────────────────────
-- dashboards  (Visualization list stored as JSONB – small embedded documents)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE dashboards (
    id              UUID    PRIMARY KEY DEFAULT gen_random_uuid(),
    title           TEXT    NOT NULL,
    visualizations  JSONB   NOT NULL DEFAULT '[]'
);
