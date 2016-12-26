import unittest

from psycopg2.extensions import connection as pg_connection

from pgawedge.connections import format_alchemy_url, get_raw_connection

from pgawedge.fixtures import AlchemySQLFixture


class TestSqlalchemyConnections(AlchemySQLFixture, unittest.TestCase):
    def test_format_alchemy_url(self):
        host = 'myhost'
        database = 'mydb'
        user = 'myuser'
        password = 'mypassword'

        expected = 'postgresql://myuser:mypassword@myhost/mydb'
        result = str(format_alchemy_url(host, database, user, password))

        self.assertEqual(expected, result)

    def test_get_raw_connection(self):
        result = get_raw_connection(self.engine)

        self.assertIsInstance(result, pg_connection)
