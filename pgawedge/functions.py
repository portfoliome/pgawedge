"""
Compilers for postgres function statements.
"""

from sqlalchemy.sql.expression import FunctionElement
from sqlalchemy.ext import compiler
from sqlalchemy.types import TIMESTAMP


__all__ = ('utcnow',)


class utcnow(FunctionElement):
    """Server side UTC timestamp."""

    type = TIMESTAMP()


@compiler.compiles(utcnow, 'postgresql')
def pg_utcnow(element, compiler, **kw):
    return "TIMEZONE('utc', CURRENT_TIMESTAMP)"
