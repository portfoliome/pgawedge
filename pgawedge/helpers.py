
import sqlalchemy as sa
from sqlalchemy import select, Table
from sqlalchemy.sql.functions import count


def get_row_count(engine, table: Table, where_clause=sa.text('')):
    """Get the row count of a table with an optional where clause."""

    row_count = count().label('row_count')

    with engine.begin() as conn:
        query = select([row_count]).select_from(table).where(where_clause)
        record = conn.execute(query).fetchone()
        number_of_rows = record[row_count.name]

    return number_of_rows
