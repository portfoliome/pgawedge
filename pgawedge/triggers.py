from functools import partial

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.schema import DDLElement


__all__ = ('CreateUpdateAtTrigger', 'DropUpdateAtTrigger',
           'UPDATE_AT_DDL_STATEMENT')


UPDATE_AT_PROCEDURE = 'set_updated_at_timestamp()'
UPDATE_AT_TRIGGER = 'trigger_column_updated_at'

# DDL to create set_updated_at_timestamp function
UPDATE_AT_DDL_STATEMENT = """\
CREATE OR REPLACE FUNCTION {procedure}  
RETURNS TRIGGER AS $$  
BEGIN  
  NEW.updated_at = NOW() AT TIME ZONE 'utc';
  RETURN NEW;
END;  
$$ LANGUAGE 'plpgsql';""".format(procedure=UPDATE_AT_PROCEDURE)


class DropTrigger(DDLElement):
    def __init__(self, name, trigger_name):
        self.name = name
        self.trigger_name = trigger_name


class CreateBeforeUpdateTrigger(DDLElement):
    def __init__(self, name, trigger_name, procedure):
        self.name = name
        self.trigger_name = trigger_name
        self.procedure = procedure


@compiles(CreateBeforeUpdateTrigger)
def compile(element, compiler, **kw):
    statement = """\
    CREATE TRIGGER {trigger_name}
      BEFORE UPDATE
      ON {qualified_name}
      FOR EACH ROW
      EXECUTE PROCEDURE {procedure}""".format(
        trigger_name=element.trigger_name, qualified_name=element.name,
        procedure=element.procedure
    )

    return statement


@compiles(DropTrigger)
def compile(element, compiler, **kw):
    statement = 'DROP TRIGGER {trigger_name} ON {table_name}'.format(
        table_name=element.name, trigger_name=element.trigger_name
    )

    return statement


CreateUpdateAtTrigger = partial(CreateBeforeUpdateTrigger,
                                procedure=UPDATE_AT_PROCEDURE,
                                trigger_name=UPDATE_AT_TRIGGER)

DropUpdateAtTrigger = partial(DropTrigger, trigger_name=UPDATE_AT_TRIGGER)
