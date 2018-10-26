import unittest

from sqlalchemy import MetaData

from pgawedge.schema import mount_declare_schema


mock_schema = 'mock_schema'


class TestMountSchema(unittest.TestCase):
    def setUp(self):
        self.schema_name = mock_schema
        self.meta = MetaData(schema=self.schema_name)

    def test_mount_declare_schema(self):
        meta = mount_declare_schema(self.meta)

        expected = f'CREATE SCHEMA IF NOT EXISTS {mock_schema}'
        result = str(meta.dispatch.before_create.listeners[0]).replace(';', '')

        self.assertEqual(expected, result)
