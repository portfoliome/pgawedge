
from foil.formatters import format_repr


class Schema:
    __slots__ = 'name',

    def __init__(self, name):
        self.name = name

    def create_statement(self):
        return 'CREATE SCHEMA IF NOT EXISTS %s;' % self.name

    def drop_statement(self):
        return 'DROP SCHEMA IF EXISTS %s CASCADE;' % self.name

    def __repr__(self):
        return format_repr(self, self.__slots__)
