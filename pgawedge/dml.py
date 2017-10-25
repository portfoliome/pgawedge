"""Helpers for creating and modifying dml statements."""

from sqlalchemy import exists, literal_column, select, Table
from sqlalchemy.dialects.postgresql import insert

from pgawedge.postgres import PG_DIALECT
from pgawedge.sql import primary_key_join

from pgawedge.helpers import filter_server_default_columns


def delete_not_exists(table, selectable):
    """Statement to delete rows in table that are not in query result."""

    delete_statement = table.delete().where(
        ~exists(
            select(
                [literal_column('1')]
            ).select_from(primary_key_join(table, selectable))
        )
    )

    return delete_statement


def create_insert_statement(table: Table, column_names=None) -> str:
    """Insert statement using subset of columns."""

    return str(compile_insert(table, column_names=column_names))


def compile_insert(table: Table, column_names=None):
    """Sqlalchemy insert statement generator with Pyformat."""

    return insert(table).compile(dialect=PG_DIALECT, column_keys=column_names)


def upsert_primary_key_statement(table: Table):
    """Insert data when primary key doesn't exist, else update."""

    ins = insert(table)
    constraint = table.primary_key.columns.keys()
    non_pkey_columns = set(
        c.name for c in table.columns.values() if c.primary_key is False
    )

    if non_pkey_columns:
        exclude = {
            k: v for k, v in ins.excluded.items() if k in non_pkey_columns
        }

        statement = ins.on_conflict_do_update(
            index_elements=constraint, set_=exclude
        )
    else:
        statement = ins.on_conflict_do_nothing(index_elements=constraint)

    return statement
