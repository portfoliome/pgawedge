"""Helpers for creating and modifying dml statements."""

from typing import Iterable

from sqlalchemy import Column, Table
from sqlalchemy.dialects.postgresql import insert

from pgawedge.postgres import PG_DIALECT


def create_insert_statement(table: Table, column_names=None) -> str:
    """Insert statement using subset of columns."""

    return str(compile_insert(table, column_names=column_names))


def filter_server_default_columns(table: Table) -> Iterable[Column]:
    """Table columns without server defaults."""

    for column in table.columns:
        if has_server_default(column):
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


def compile_insert(table: Table, column_names=None):
    """Sqlalchemy insert statement generator with Pyformat."""

    return insert(table).compile(dialect=PG_DIALECT, column_keys=column_names)
