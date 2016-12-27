"""
Compiler objects for postgres data definition language.
"""

from sqlalchemy.sql.expression import Executable, ClauseElement
from sqlalchemy.ext.compiler import compiles


class CreateTableAs(Executable, ClauseElement):

    def __init__(self, name, query):
        self.name = name
        self.query = query


@compiles(CreateTableAs, "postgresql")
def _create_table_as(element, compiler, **kw):
    return "CREATE TABLE %s AS %s" % (
        element.name,
        compiler.process(element.query)
    )
