import uuid
from inspect import signature

from sqlalchemy.types import TypeEngine
from sqlalchemy.dialects.postgresql import (
    ARRAY, BOOLEAN, CHAR, DATE, FLOAT, JSON,
    JSONB, INTEGER, TEXT, TIMESTAMP, VARCHAR
)
from sqlalchemy.types import NullType


class UUID(TypeEngine):
    """PostgreSQL UUID type.

    Represents the UUID column type for Postgres.

    Implementation removes need to specify UUID(as_uuid=True)
    each time, but the implementation borrows heavily from
    sqlalchemy Postgres UUID type implementation.
    """

    __visit_name__ = 'UUID'

    def __init__(self):
        """Construct a UUID type."""

    def bind_processor(self, dialect):
        def process(value):
            if value is not None:
                value = str(value)
            return value

        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            if value is None or isinstance(value, uuid.UUID):
                value = value
            else:
                value = uuid.UUID(value)
            return value

        return process


def get_type_attributes(data_type):
    """Get SQL data type attributes.

    Returns data type's attribute values for comparison. Intent is
    to coverage basic Postgresql types. Types such as ARRAYs would
    need to have additional calls if there are nested data type objects.
    
    Only supports positional_or_keyword arguments like Sqlalchemy.
    """

    sig = signature(data_type.__init__)

    for attribute, parameter in sig.parameters.items():
        if parameter.kind is parameter.POSITIONAL_OR_KEYWORD:
            value = getattr(data_type, attribute)

            if parameter.default is sig.empty:
                yield value
            elif parameter.default != value:
                yield (attribute, value)
