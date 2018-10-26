
import sqlalchemy as sa
from sqlalchemy import select, Table, Column
from sqlalchemy.sql import Selectable
from sqlalchemy.sql.functions import count
from typing import Iterable, List

from sqlalchemy.sql.util import find_tables

from pgawedge.data_types import get_type_attributes
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


def filter_server_default_columns(table: Table) -> Iterable[Column]:
    """Table columns without server defaults."""

    for column in table.columns:
        if has_server_default(column) is False:
            yield column


def has_server_default(column: Column) -> bool:
    """Column has server default value."""

    if column.server_default or column.server_onupdate:
        return True
    else:
        return False


def has_default(column: Column) -> bool:
    """Column has server or Sqlalchemy default value."""

    if has_server_default(column) or column.default:
        return True
    else:
        return False


def is_autoincrement_column(column: Column) -> bool:
    """Column is auto-incrementing column."""

    return True if column.autoincrement is True else False


def get_required_columns(table: Table,
                         has_default_hook=has_server_default) -> Iterable[Column]:
    """Columns required for insert, update, and upsert queries.

    Use case is typically for generating an SQL insert string. Therefore,
    columns with Python/Sqlalchemy default values or clauses are included
    as required with default hook.
    """

    for column in table.columns:
        not_nullable = column.nullable is False
        no_default = has_default_hook(column) is False
        not_autoincrement = is_autoincrement_column(column) is False

        if not_nullable and no_default and not_autoincrement:
            yield column


def find_selectable_dependencies(selectable: Selectable) -> List[Table]:
    """Find the tables used in a select query."""

    return (table for table in set(find_tables(selectable)))
