import unittest

from sqlalchemy import (
    Column, column, MetaData, Table, select, table, VARCHAR
)

from pgawedge.sql import primary_key_join


class TestPrimaryKeyJoin(unittest.TestCase):

    def setUp(self):
        self.select_table = table('other_table',
                                  column('id'), column('id2'))
        self.statement = select([self.select_table]).cte('c')

    def test_single_key(self):
        t = Table('t', MetaData(), Column('id', VARCHAR(2), primary_key=True))
        join_statement = primary_key_join(t, self.statement)

        expected = 't JOIN c ON t.id = c.id'
        result = str(join_statement)

        self.assertEqual(expected, result)

    def test_composite_key(self):
        t = Table('t', MetaData(),
                  Column('id', VARCHAR(2), primary_key=True),
                  Column('id2', VARCHAR(2), primary_key=True))
        join_statement = primary_key_join(t, self.statement)

        expected = 't JOIN c ON t.id = c.id AND t.id2 = c.id2'
        result = str(join_statement)

        self.assertEqual(expected, result)
