"""
sqlalchemy data types and dialects for postgres, as well as
sqlalchemy configuration utilities for postgres conventions.
"""

from datetime import date, datetime
import uuid

from sqlalchemy.dialects.postgresql import (
    ARRAY, BOOLEAN, CHAR, DATE, FLOAT, JSON,
    JSONB, INTEGER, TEXT, TIMESTAMP, UUID, VARCHAR
)
from sqlalchemy.databases import postgresql as postgres_dialect
from sqlalchemy.types import NullType


PG_DIALECT = postgres_dialect.dialect()
RESERVED_WORDS = frozenset(postgres_dialect.RESERVED_WORDS)

CONSTRAINT_CONVENTIONS = {
    "ix": '%(column_0_label)s_idx',
    "uq": "%(table_name)s_%(column_0_name)s_key",
    "ck": "%(table_name)s_%(constraint_name)s_check",
    "fk": "%(table_name)s_%(column_0_name)s_%(referred_table_name)s_fkey",
    "pk": "%(table_name)s_pkey"
}

ALCHEMY_TO_PYTHON_DATA_TYPE = {
    ARRAY: list,
    INTEGER: int,
    BOOLEAN: bool,
    FLOAT: float,
    CHAR: str,
    DATE: date,
    VARCHAR: str,
    TEXT: str,
    NullType: None,
    UUID: uuid.UUID,
    TIMESTAMP: datetime
}
