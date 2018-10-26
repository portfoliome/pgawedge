from sqlalchemy.schema import CreateColumn
from sqlalchemy.sql.ddl import _CreateDropBase
from sqlalchemy.ext.compiler import compiles
from sqlalchemy import Column, event, MetaData, Table
from sqlalchemy.sql import Selectable

from pgawedge.helpers import find_selectable_dependencies


class _View(Table):
    __visit_name__ = 'view'
    is_view = True


def View(name: str, metadata, selectable: Selectable, replace=True,
         cascade=False):

    v = _View(name, metadata)
    _meta = MetaData(schema=metadata.schema,
                     naming_convention=metadata.naming_convention)
    table = Table(name, _meta)

    for c in selectable.c:
        table.append_column(Column(c.name, c.type))

    event.listen(
        metadata, 'after_create', CreateView(v, selectable, replace=replace)
    )
    event.listen(
        metadata, 'before_drop', DropView(v, if_exists=True, cascade=cascade)
    )

    for dependency in find_selectable_dependencies(selectable):
        table.add_is_dependent_on(dependency)
        v.add_is_dependent_on(dependency)

    return table


class CreateView(_CreateDropBase):
    """
    Prepares a CREATE VIEW statement.

    Parameters
    ----------
    replace:
        If True, this definition will replace an existing definition.
        Otherwise, an exception will be raised if the view exists.
    """

    __visit_name__ = 'create_view'

    def __init__(self, element: _View, selectable: Selectable, on=None,
                 bind=None, replace=True):

        super(CreateView, self).__init__(element, on=on, bind=bind)
        self.columns = [CreateColumn(column) for column in element.columns]
        self.selectable = selectable
        self.replace = replace


@compiles(CreateView)
def visit_create_view(create, compiler, **kw):
    view = create.element
    preparer = compiler.dialect.identifier_preparer
    text = '\nCREATE '

    if create.replace:
        text += 'OR REPLACE '
    text += 'VIEW %s ' % preparer.format_table(view)

    if create.columns:
        column_names = [preparer.format_column(col.element)
                        for col in create.columns]
        text += '(%s)' % ', '.join(column_names)

    text += 'AS %s\n\n' % compiler.sql_compiler.process(create.selectable,
                                                        literal_binds=True)

    return text


class DropView(_CreateDropBase):
    """
    Prepares a DROP VIEW statement.

    cascade: Drop any dependent views.
    if_exists: Do nothing if the view does not exist, else raise exception.
    """

    __visit_name__ = 'drop_view'

    def __init__(self, element: _View, on=None, bind=None,
                 cascade: bool=False, if_exists: bool=False):
        super(DropView, self).__init__(element, on=on, bind=bind)
        self.cascade = cascade
        self.if_exists = if_exists


@compiles(DropView)
def compile(drop, compiler, **kw):
    text = "\nDROP VIEW "
    if drop.if_exists:
        text += "IF EXISTS "
    text += compiler.preparer.format_table(drop.element)
    if drop.cascade:
        text += " CASCADE"
    return text
