import unittest

import sqlalchemy as sa

from pgawedge.alchemy import (declare_schema, drop_schema,
                              sa_column_dict_to_column, reset_schema)
from pgawedge.fixtures import AlchemySQLFixture


SELECT_SCHEMA_QUERY = """\
SELECT schema_name FROM information_schema.schemata
WHERE schema_name=%s"""


def schema_exists(engine, schema_name):
    with engine.begin() as conn:
        result = conn.execute(SELECT_SCHEMA_QUERY, (schema_name,)).fetchone()

        return result is not None


class TestDeclareSchema(AlchemySQLFixture, unittest.TestCase):

    schema_name = 'declare_schema'

    def test_declare_schema(self):
        declare_schema(self.engine, schema_name=self.schema_name)

        self.assertTrue(schema_exists(self.engine, self.schema_name))

    @classmethod
    def _cleanup_db(cls):
        drop_statement = 'DROP SCHEMA {} CASCADE'.format(cls.schema_name)

        with cls.engine.begin() as conn:
            conn.execute(sa.text(drop_statement))


class TestSchemaUtilities(AlchemySQLFixture, unittest.TestCase):

    def setUp(self):
        self.schema_name = 'schema_helpers'
        with self.engine.begin() as conn:
            conn.execute(sa.text('CREATE SCHEMA {}'.format(self.schema_name)))

    def test_drop_schema(self):
        drop_schema(self.engine, self.schema_name)

        self.assertFalse(schema_exists(self.engine, self.schema_name))

    def test_reset_schema(self):
        # Create a table then reset
        tablename = 'mock_table'
        table = sa.Table(tablename, self.class_meta, schema=self.schema_name)
        table.create(self.engine)

        reset_schema(self.engine, self.schema_name)

        self.assertTrue(schema_exists(self.engine, self.schema_name))

        meta = sa.MetaData()
        meta.reflect(self.engine, schema=self.schema_name)
        tablenames = set([t.name for t in meta.sorted_tables])

        self.assertFalse(tablename in tablenames)

    def tearDown(self):
        with self.engine.begin() as conn:
            conn.execute(
                'DROP SCHEMA IF EXISTS {} CASCADE'.format(self.schema_name)
            )


class TestAlchemySchema(unittest.TestCase):

    def test_sa_column_dict_to_column(self):
        column_name = 'my_mock_column'
        column_type = sa.CHAR(length=2)
        nullable = False

        attributes = {
            'autoincrement': False, 'default': None, 'name': column_name,
            'nullable': nullable, 'type': column_type
        }

        result_column = sa_column_dict_to_column(attributes)
        result = {attribute: getattr(result_column, attribute)
                  for attribute in attributes.keys()}

        self.assertEqual(attributes, result)
