import unittest

from postpy.fixtures import PostgresStatementFixture
from sqlalchemy import Column, MetaData, Table, VARCHAR

from pgawedge.dml import (
    create_insert_statement, delete_not_exists, upsert_primary_key_statement
)
from pgawedge.postgres import PG_DIALECT


class TestDeleteNotExists(PostgresStatementFixture, unittest.TestCase):
    def setUp(self):
        self.meta_schema = MetaData()
        self.primary_key_name = 'id'
        self.table = Table('foo', self.meta_schema,
                           Column(self.primary_key_name, VARCHAR(60),
                                  primary_key=True))
        self.refresh_table = Table('foo_refresh', self.meta_schema,
                                   Column(self.primary_key_name, VARCHAR(60)))

    def test_build_delete_statement(self):
        statement = delete_not_exists(self.table, self.refresh_table)

        expected = ('DELETE FROM foo WHERE NOT (EXISTS (SELECT 1 '
                    'FROM foo JOIN foo_refresh ON foo.id = foo_refresh.id))')
        result = str(statement.compile(dialect=PG_DIALECT))

        self.assertSQLStatementEqual(expected, result)


class TestInserts(unittest.TestCase):
    def setUp(self):
        self.table_name = 'foobar'
        self.meta_schema = MetaData()
        self.primary_key = Column('foo', VARCHAR(60), primary_key=True)
        self.table = Table(self.table_name, self.meta_schema,
                           self.primary_key,
                           Column('bar', VARCHAR(20), nullable=False))

    def test_create_insert_statement(self):
        expected = 'INSERT INTO foobar (foo, bar) VALUES (%(foo)s, %(bar)s)'
        result = create_insert_statement(self.table)

        self.assertEqual(expected, result)

    def test_upsert_primary_key(self):
        statement = upsert_primary_key_statement(self.table)

        expected = ('INSERT INTO foobar (foo, bar) VALUES (%(foo)s, %(bar)s)'
                    ' ON CONFLICT (foo) DO UPDATE SET bar = excluded.bar')
        result = str(statement.compile(dialect=PG_DIALECT))

        self.assertEqual(expected, result)




