import unittest

import sqlalchemy as sa
from sqlalchemy import column, select, table

from pgawedge.fixtures import AlchemySQLFixture
from pgawedge.helpers import get_row_count, stringify_query


class TestGetRowCount(AlchemySQLFixture, unittest.TestCase):

    def setUp(self):
        self.meta_schema = sa.MetaData()
        self.column_name = 'my_rows'
        self.records = list(range(0, 5))
        self.table = sa.Table(
            'row_count_test', self.meta_schema,
            sa.Column(self.column_name, sa.INTEGER, autoincrement=False)
        )
        self.meta_schema.create_all(self.engine)
        self.engine.execute(self.table.insert().values(
            [{self.column_name: i} for i in self.records])
        )

    def test_get_row_count(self):
        expected = len(self.records)
        result = get_row_count(self.engine, self.table)

        self.assertEqual(expected, result)

    def test_get_row_count_conditional(self):
        cutoff = 3
        where = self.table.c.my_rows >= cutoff

        expected = len([r for r in self.records if r >= cutoff])
        result = get_row_count(self.engine, self.table, where_clause=where)

        self.assertEqual(expected, result)

    def tearDown(self):
        self.meta_schema.drop_all(self.engine)


class TestCompilers(unittest.TestCase):

    def setUp(self):
        self.columns = [column('x'), column('y')]
        self.test_table = table('test_table', *self.columns)

    def test_stringify_query(self):
        query = select('*').select_from(self.test_table).where(
            self.test_table.c.x > 50
        )

        expected = 'SELECT * FROM test_table WHERE test_table.x > 50'
        result = stringify_query(query).replace('\n', '')

        self.assertEqual(expected, result)
