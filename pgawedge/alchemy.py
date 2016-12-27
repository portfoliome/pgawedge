import sqlalchemy as sa
from sqlalchemy import MetaData

from postpy import base as pg

from pgawedge.postgres import CONSTRAINT_CONVENTIONS


__all__ = ('declare_schema', 'drop_schema', 'sa_meta_schema',)


PRIMARY_KEY = 'primary_key'


def sa_meta_schema(schema_name=None):
    return MetaData(schema=schema_name,
                    naming_convention=CONSTRAINT_CONVENTIONS)


def declare_schema(engine, schema_name):
    """Execute declaration for schema space."""

    schema = pg.Schema(schema_name)
    statement = sa.DDL(schema.create_statement())

    with engine.begin() as conn:
        conn.execute(statement)


def drop_schema(engine, schema_name):
    """Execute teardown for schema space."""

    schema = pg.Schema(schema_name)
    statement = sa.DDL(schema.drop_statement())

    with engine.begin() as conn:
        conn.execute(statement)


def reset_schema(engine, schema_name):
    """Execute drop and declaration for schema space."""

    schema = pg.Schema(schema_name)
    drop_statement = sa.DDL(schema.drop_statement())
    create_statement = sa.DDL(schema.create_statement())

    with engine.begin() as conn:
        conn.execute(drop_statement)
        conn.execute(create_statement)


class AlchemySchema:
    def __init__(self, meta_schema):
        self.meta_schema = meta_schema

    def create_schema(self, engine):
        self.meta_schema.create_all(engine)

    def reflect_schema(self, engine):
        self.meta_schema.reflect(engine)

    def drop_schema(self, engine):
        self.meta_schema.drop_all(engine)


def sa_column_dict_to_column(attributes):
    """Convert reflected column attribute dictionary into Column object."""

    # convert dictionary keys to sqlalchemy column parameters
    key_name_swap = {'type': 'type_'}

    column_parameters = {
        key_name_swap.get(k, k): v for k, v in attributes.items()
        }

    return sa.Column(**column_parameters)
