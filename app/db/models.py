from datetime import datetime
from typing import Optional, Annotated
from uuid import uuid4, UUID

import pytz
from pydantic import PlainSerializer
from pydantic_core import core_schema
from sqlalchemy import DateTime
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

UTCDateTime = Annotated[
    datetime,
    PlainSerializer(
        lambda _datetime: _datetime.strftime("%Y-%m-%dT%H:%M:%SZ") if isinstance(_datetime, datetime) else _datetime,
        return_type=str),
]


class Base(DeclarativeBase):
    pass


class PydanticCompatMixin:
    """Makes SQLAlchemy models serializable by FastAPI/Pydantic via model_dump()."""

    @classmethod
    def __get_pydantic_core_schema__(cls, source_type, handler):
        return core_schema.no_info_plain_validator_function(
            lambda v: v,
            serialization=core_schema.plain_serializer_function_ser_schema(
                lambda v: v.model_dump() if hasattr(v, "model_dump") else v,
                info_arg=False,
            ),
        )

    def model_dump(self) -> dict:
        raise NotImplementedError


class BaseEntity(PydanticCompatMixin, Base):
    __abstract__ = True

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True)

    def __init__(self, **kwargs):
        if "id" not in kwargs:
            kwargs["id"] = uuid4()
        super().__init__(**kwargs)

    @property
    def uuid(self) -> UUID:
        return self.id


class EditableEntity(BaseEntity):
    __abstract__ = True

    imported_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    def __init__(self, **kwargs):
        if "imported_at" not in kwargs:
            kwargs["imported_at"] = datetime.now(tz=pytz.UTC)
        super().__init__(**kwargs)


class SNMEntityMixin:
    snm_key: Mapped[Optional[str]] = mapped_column(nullable=True, default=None)
    duplication_key: Mapped[Optional[UUID]] = mapped_column(
        PG_UUID(as_uuid=True), nullable=True, default=None
    )
    marked_for_removal: Mapped[bool] = mapped_column(default=False)