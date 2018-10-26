from postpy import base as pg
from sqlalchemy import DDL, event


def mount_declare_schema(target_schema):
    schema = pg.Schema(target_schema.schema)
    ddl_statement = DDL(schema.create_statement())
    event.listen(target_schema, 'before_create',
                 ddl_statement.execute_if(dialect='postgresql'))

    return target_schema
