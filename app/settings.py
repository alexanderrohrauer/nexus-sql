from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    evaluation_mode: bool = False
    default_batch_size: int = 20
    openalex_batch_size: int = 20
    optimized_insert: bool = False
    evaluation_path: str = "./evaluation/data"
    db_connection_string: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/nexus_dev"
    db_connection_string_sync: str = "postgresql+psycopg2://postgres:postgres@localhost:5432/nexus_dev"

    model_config = SettingsConfigDict(env_file=".env")


@lru_cache
def get_settings() -> Settings:
    return Settings()