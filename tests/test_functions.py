import unittest

import sqlalchemy as sa
from sqlalchemy.schema import CreateColumn

from pgawedge.functions import utcnow
from .common import PostgresStatementFixture
from .alchemy_fixtures import create_mock_engine


class TestAlchemyUTCNow(PostgresStatementFixture, unittest.TestCase):

    def setUp(self):
        self.engine = create_mock_engine()

    def test_utcnow_compile(self):
        column = sa.Column('mycol', sa.TIMESTAMP(timezone=True),
                           server_default=utcnow())
        create_column = CreateColumn(column)

        expected = "mycol TIMESTAMP WITH TIME ZONE DEFAULT TIMEZONE('utc', CURRENT_TIMESTAMP)"
        result = str(create_column.compile(self.engine))

        self.assertSQLStatementEqual(expected, result)
