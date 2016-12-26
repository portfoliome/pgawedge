import unittest

import sqlalchemy as sa

from pgawedge.ddl import CreateTableAs
from pgawedge.fixtures import AlchemySQLFixture


class TestCreateTableAs(AlchemySQLFixture, unittest.TestCase):

    def setUp(self):
        self.meta = sa.MetaData()
        self.table_name = 't1'
        self.table = sa.Table(self.table_name, self.meta,
                              sa.Column('x', sa.Integer),
                              sa.Column('y', sa.Integer))
        self.data = [{"x": 1, "y": 2}, {"x": 11, "y": 3}]

    def test_create_table_as(self):

        table_name = 'table2'
        select_query = "select x, y from {table}".format(table=table_name)

        with self.conn.begin() as trans:
            self.table.create(self.conn)
            self.conn.execute(self.table.insert(), self.data)

            s = sa.select([self.table]).where(self.table.c.x > 10)
            self.conn.execute(CreateTableAs(table_name, s))

            expected = [(self.data[1]['x'], self.data[1]['y'])]
            result = self.conn.execute(select_query).fetchall()

            trans.rollback()

        self.assertEqual(expected, result)
