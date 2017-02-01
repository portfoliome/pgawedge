import unittest
import uuid

import sqlalchemy as sa

from pgawedge.fixtures import AlchemySQLFixture
from pgawedge.data_types import UUID


class TestAlchemyDataTypes(AlchemySQLFixture, unittest.TestCase):

    def setUp(self):
        self.meta_data = sa.MetaData()
        self.id_column = 'my_id'
        self.table_name = 'default_column_table'
        self.data_column = 'my_data'
        self.table = self.get_table()
        self._initialize_data()

    def test_uuid_type(self):
        value = self.get_id_value()

        self.assertTrue(isinstance(value, uuid.UUID))

    def test_uuid_type_psycopg2(self):
        from postpy.uuids import register_client
        register_client()

        value = self.get_id_value()

        self.assertTrue(isinstance(value, uuid.UUID))

    def get_table(self):
        return sa.Table(self.table_name, self.meta_data,
                        sa.Column(self.id_column, UUID,
                                  default=uuid.uuid4))

    def get_id_value(self):
        with self.engine.begin() as conn:
            data = dict(conn.execute(self.table.select()).fetchone())

        return data[self.id_column]

    def _initialize_data(self):
        self.meta_data.create_all(self.engine)

        with self.engine.begin() as conn:
            conn.execute(self.table.insert().values([{}]))

    def tearDown(self):
        self.meta_data.drop_all(self.engine)
