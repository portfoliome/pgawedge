"""
sqlalchemy engine and connection utilities for postgres.
"""

import os

import sqlalchemy as sa


__all__ = ('create_alchemy_engine',)


def create_alchemy_engine(host=None, database=None, user=None, password=None,
                          **kwargs):
    """sqlalchemy engine."""

    host = host or os.environ['PGHOST']
    database = database or os.environ['PGDATABASE']
    user = user or os.environ['PGUSER']
    password = password or os.environ['PGPASSWORD']

    sa_url = format_alchemy_url(host=host, database=database,
                                user=user, password=password)

    return sa.create_engine(sa_url, **kwargs)


def format_alchemy_url(host, database, user, password):
    """Format psycopg2 connection parameters to Sqlalchemy engine."""

    sa_url = sa.engine.url.URL('postgresql', host=host, database=database,
                               username=user, password=password)
    return sa_url


def get_raw_connection(engine):
    """psycopg2 connection."""

    alchemy_conn = engine.connect()
    alchemy_conn.detach()
    raw_connection = alchemy_conn.connection.connection

    return raw_connection
