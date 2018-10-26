import unittest

from sqlalchemy import (
    BOOLEAN, Column, INTEGER, MetaData, Table, TIMESTAMP, VARCHAR,
    column, select, table, text
)

from pgawedge.fixtures import AlchemySQLFixture
from pgawedge.helpers import (
    get_required_columns, get_row_count, filter_server_default_columns,
    stringify_query, has_server_default, has_default, is_autoincrement_column,
    find_selectable_dependencies
)


def mock_default():
    return True


class TestGetRowCount(AlchemySQLFixture, unittest.TestCase):

    def setUp(self):
        self.meta_schema = MetaData()
        self.column_name = 'my_rows'
        self.records = list(range(0, 5))
        self.table = Table(
            'row_count_test', self.meta_schema,
            Column(self.column_name, INTEGER, autoincrement=False)
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


class TestRequiredColumns(unittest.TestCase):
    def setUp(self):
        self.table_name = 'foobar'
        self.meta_schema = MetaData()
        self.primary_key = Column('foo', VARCHAR(60), primary_key=True)
        self.autoincrement_column = Column('total', INTEGER, autoincrement=True)
        self.server_default_column = Column(
            'created_at', TIMESTAMP, nullable=False,
            server_default=text('NOW()')
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
        result = list(filter_server_default_columns(self.table))

        self.assertNotIn(self.server_default_column, result)

    def test_is_autoincrement_column(self):
        self.assertTrue(is_autoincrement_column(self.autoincrement_column))

    def test_get_required_columns(self):
        expected = set(['foo', 'bar', 'my_default'])
        result = set(c.name for c in get_required_columns(self.table))

        self.assertEqual(expected, result)


class TestSelectableDependencies(unittest.TestCase):
    def test_find_selectable_dependencies(self):
        meta = MetaData()
        t1 = Table('foo', meta, Column('x'))
        t2 = Table('bar', meta, Column('x'))
        selectable = select([t1]).select_from(t1.join(t2, t1.c.x == t2.c.x))

        expected = set([t1, t2])
        result = set(find_selectable_dependencies(selectable))

        self.assertEqual(expected, result)
