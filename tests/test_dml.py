import unittest

import sqlalchemy as sa
from sqlalchemy import (
    BOOLEAN, Column, INTEGER, Table, MetaData, TIMESTAMP, VARCHAR
)

from pgawedge.dml import (
    create_insert_statement, get_required_columns,
    filter_server_default_columns, has_default, has_server_default,
    is_autoincrement_column
)


def mock_default():
    return True


class TestRequiredColumns(unittest.TestCase):
    def setUp(self):
        self.table_name = 'foobar'
        self.meta_schema = MetaData()
        self.primary_key = Column('foo', VARCHAR(60), primary_key=True)
        self.autoincrement_column = Column('total', INTEGER, autoincrement=True)
        self.server_default_column = Column(
            'created_at', TIMESTAMP, nullable=False,
            server_default=sa.text('NOW()')
        )
        self.default_column = Column('my_default', BOOLEAN, nullable=False,
                                     default=mock_default())
        self.table = Table(
            self.table_name, self.meta_schema,
            self.primary_key,
            Column('bar', VARCHAR(20), nullable=False),
            self.default_column,
            self.autoincrement_column,
            self.server_default_column
        )

    def test_has_server_default(self):
        self.assertTrue(has_server_default(self.server_default_column))

    def test_has_default(self):
        self.assertTrue(has_default(self.default_column))
        self.assertFalse(has_default(self.primary_key))

    def test_filter_server_default_columns(self):
        expected = [self.server_default_column]
        result = list(filter_server_default_columns(self.table))

        self.assertEqual(expected, result)

    def test_is_autoincrement_column(self):
        self.assertTrue(is_autoincrement_column(self.autoincrement_column))

    def test_get_required_columns(self):
        expected = set(['foo', 'bar', 'my_default'])
        result = set(c.name for c in get_required_columns(self.table))

        self.assertEqual(expected, result)

    def test_create_insert_statement(self):
        expected = (
            'INSERT INTO foobar (foo, bar, my_default, total, created_at)'
            ' VALUES (%(foo)s, %(bar)s, %(my_default)s, %(total)s, %(created_at)s)'
        )
        result = create_insert_statement(self.table)

        self.assertEqual(expected, result)
