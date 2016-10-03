import unittest

from pga.db import Schema
from .common import PostgresStatementFixture


class TestSchemaStatements(PostgresStatementFixture, unittest.TestCase):

    def setUp(self):
        self.schema_name = 'test_schema'
        self.schema = Schema(self.schema_name)

    def test_create_schema_statement(self):
        expected = 'CREATE SCHEMA IF NOT EXISTS test_schema;'
        result = self.schema.create_statement()

        self.assertSQLStatementEqual(expected, result)

    def test_drop_schema_statement(self):
        expected = 'DROP SCHEMA IF EXISTS test_schema CASCADE;'
        result = self.schema.drop_statement()

        self.assertSQLStatementEqual(expected, result)
