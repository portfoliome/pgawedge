import unittest

from postpy.fixtures import PostgresStatementFixture

from pgawedge.triggers import CreateBeforeUpdateTrigger, DropTrigger


class TestTriggerCompilers(PostgresStatementFixture, unittest.TestCase):

    def setUp(self):
        self.name = 'my_table'
        self.trigger_name = 'my_trigger'
        self.procedure = 'my_procedure'

    def test_create_trigger(self):
        expected = ('CREATE TRIGGER my_trigger BEFORE UPDATE'
                    ' ON my_table FOR EACH ROW'
                    ' EXECUTE PROCEDURE my_procedure')
        result = str(CreateBeforeUpdateTrigger(self.name,
                                               self.trigger_name,
                                               self.procedure))

        self.assertSQLStatementEqual(expected, result)

    def test_drop_trigger(self):
        expected = 'DROP TRIGGER my_trigger ON my_table'
        result = str(DropTrigger(self.name, self.trigger_name))

        self.assertSQLStatementEqual(expected, result)
