import unittest

import sqlalchemy as sa
from sqlalchemy import MetaData

from pgawedge.views import CreateView, DropView, _View


class TestViewDDL(unittest.TestCase):

    def setUp(self):
        self.name = 'foobar'
        self.meta = MetaData()
        t = sa.table('foo', sa.column('bar'))
        self.selectable = sa.select([t])
        self.view = _View(self.name, self.meta)

    def test_create_view(self):
        compiler = CreateView(self.view, self.selectable)

        expected = 'CREATE OR REPLACE VIEW foobar AS SELECT foo.bar FROM foo'
        result = str(compiler).replace('\n', '')

        self.assertEqual(expected, result)

    def test_drop_view(self):
        compiler = DropView(self.view)

        expected = 'DROP VIEW foobar'
        result = str(compiler).replace('\n', '')

        self.assertEqual(expected, result)
