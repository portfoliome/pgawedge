
import sqlalchemy as sa
from sqlalchemy import select, Table
from sqlalchemy.sql.functions import count

from pgawedge.postgres import PG_DIALECT


def get_row_count(engine, table: Table, where_clause=sa.text('')):
    """Get the row count of a table with an optional where clause."""

    row_count = count().label('row_count')

    with engine.begin() as conn:
        query = select([row_count]).select_from(table).where(where_clause)
        record = conn.execute(query).fetchone()
        number_of_rows = record[row_count.name]

    return number_of_rows


def stringify_query(alchemy_statement):
    """Compile alchemy query into PyFormat string."""

    return str(alchemy_statement.compile(
        dialect=PG_DIALECT,
        compile_kwargs={"literal_binds": True}
    ))


def compile_expression(alchemy_statement):
    return sa.text(stringify_query(alchemy_statement))
