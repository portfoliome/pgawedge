import unittest

import sqlalchemy as sa
from sqlalchemy.schema import CreateColumn
from postpy.fixtures import PostgresStatementFixture

from pgawedge.functions import gen_random_uuid, utcnow, uuid_generate_v1mc
from pgawedge.fixtures import create_mock_engine

from pgawedge.data_types import UUID
from postpy.uuids import register_client


class TestColumnCompilers(PostgresStatementFixture, unittest.TestCase):

    def setUp(self):
        register_client()
        self.engine = create_mock_engine()

    def test_utcnow_compile(self):
        column = sa.Column('mycol', sa.TIMESTAMP(timezone=True),
                           server_default=utcnow())
        expected = "mycol TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc', CURRENT_TIMESTAMP)"

        self._check_column_ddl(expected, column)

    def test_gen_random_uuid(self):
        column = sa.Column('mycol', UUID, server_default=gen_random_uuid())
        expected = 'mycol UUID DEFAULT gen_random_uuid()'

        self._check_column_ddl(expected, column)

    def test_uuid_generate_v1mc(self):
        column = sa.Column('mycol', UUID, server_default=uuid_generate_v1mc())
        expected = 'mycol UUID DEFAULT uuid_generate_v1mc()'

        self._check_column_ddl(expected, column)

    def _check_column_ddl(self, expected, column):
        create_column = CreateColumn(column)
        result = str(create_column.compile(self.engine))

        self.assertSQLStatementEqual(expected, result)
