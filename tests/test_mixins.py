import unittest

from sqlalchemy import Column, INTEGER, MetaData, VARCHAR
from sqlalchemy.ext.declarative import declarative_base

from pgawedge.fixtures import AlchemySQLFixture
from pgawedge.mixins import AuditMixin
from pgawedge.triggers import UPDATE_AT_DDL_STATEMENT, UPDATE_AT_PROCEDURE


SCHEMA_NAME = 'my_schema'
meta = MetaData(schema=SCHEMA_NAME)
Base = declarative_base(metadata=meta)


def get_mock_insert_data():
    return [{'id': id_, 'data': data}
            for id_, data in zip(['a', 'b', 'c'], [1, 2, 3])]


class MyTable(Base, AuditMixin):

    __tablename__ = 'my_table'

    id = Column(VARCHAR(20), primary_key=True)
    data = Column(INTEGER)


class TestAuditMixin(AlchemySQLFixture, unittest.TestCase):

    def setUp(self):
        self.my_model_table = MyTable()
        self.my_table = self.my_model_table.__table__
        self.data = get_mock_insert_data()
        self.my_table.create(self.engine)

    @classmethod
    def _prep_db(cls):
        cls.engine.execute('CREATE SCHEMA {}'.format(SCHEMA_NAME))
        cls.engine.execute(UPDATE_AT_DDL_STATEMENT)

    def tearDown(self):
        self.my_table.drop(self.engine)

    @classmethod
    def _cleanup_db(cls):
        cls.engine.execute('DROP SCHEMA IF EXISTS {} CASCADE'.format(SCHEMA_NAME))
        cls.engine.execute('DROP FUNCTION IF EXISTS {} CASCADE'.format(UPDATE_AT_PROCEDURE))

    def test_audit_mixin_update(self):
        self.insert_records()
        results = self.get_records()

        # update specific record
        id_ = results[0]['id']
        initial_time = results[0]['updated_at']
        self.update_record(id_, None)

        new_results = self.get_records()
        new_record = next(r for r in new_results if r['id'] == id_)
        updated_time = new_record['updated_at']
        created_time = new_record['created_at']

        # check that updated_at is greater than initial timestamps
        self.assertTrue(updated_time > initial_time)
        self.assertTrue(updated_time > created_time)

    def insert_records(self):
        ins = self.my_table.insert()
        self.engine.execute(ins.values(self.data))

    def update_record(self, id_, value):
        statement = self.my_table.update().where(
            self.my_table.c.id == id_
        ).values(data=value)

        self.engine.execute(statement)

    def get_records(self):
        return [
            dict(r) for r in self.engine.execute(self.my_table.select()).fetchall()
        ]








