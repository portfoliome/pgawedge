from sqlalchemy import join, and_


def primary_key_join(table, statement):
    """Joins table and statement based on table's primary key column names."""

    join_keys = (primary_key == statement.columns[primary_key.name]
                 for primary_key in table.primary_key.columns)

    return join(table, statement, and_(*join_keys))
