"""
Compilers for postgres function statements.
"""

from postpy.uuids import random_uuid_function, uuid_sequence_function
from sqlalchemy.sql.expression import FunctionElement
from sqlalchemy.ext import compiler
from sqlalchemy.types import TIMESTAMP

from pgawedge.data_types import UUID


__all__ = ('utcnow',)


class utcnow(FunctionElement):
    """Server side UTC timestamp."""

    type = TIMESTAMP()


@compiler.compiles(utcnow, 'postgresql')
def pg_utcnow(element, compiler, **kw):
    return "TIMEZONE('utc', CURRENT_TIMESTAMP)"


class gen_random_uuid(FunctionElement):
    """Server side random UUID4."""

    type = UUID()


@compiler.compiles(gen_random_uuid, 'postgresql')
def pg_gen_random_uuid(element, compiler, **kw):
    """TODO support schema names"""

    return random_uuid_function()


class uuid_generate_v1mc(FunctionElement):
    """Server side sequential UUID4."""

    type = UUID()


@compiler.compiles(uuid_generate_v1mc, 'postgresql')
def pg_uuid_generate_v1mc(element, compiler, **kw):

    return uuid_sequence_function()
