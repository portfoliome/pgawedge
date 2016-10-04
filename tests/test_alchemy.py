import unittest

import sqlalchemy as sa

from pgawedge.alchemy import sa_column_dict_to_column


class TestAlchemySchema(unittest.TestCase):

    def test_sa_column_dict_to_column(self):
        column_name = 'my_mock_column'
        column_type = sa.CHAR(length=2)
        nullable = False

        attributes = {
            'autoincrement': False, 'default': None, 'name': column_name,
            'nullable': nullable, 'type': column_type
        }

        result_column = sa_column_dict_to_column(attributes)
        result = {attribute: getattr(result_column, attribute)
                  for attribute in attributes.keys()}

        self.assertEqual(attributes, result)
