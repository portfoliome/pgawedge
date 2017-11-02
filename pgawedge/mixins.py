from sqlalchemy import Column, event, TIMESTAMP
from sqlalchemy.schema import FetchedValue

from pgawedge.functions import utcnow
from pgawedge.triggers import CreateUpdateAtTrigger, DropUpdateAtTrigger


class AuditMixin(object):
    """Server side created_at and updated_at utc table audit column mixin.
    
    Notes
    -----
    - The stored procedure should be created prior to table creation.
      pgawedge.triggers.UPDATE_AT_DDL_STATEMENT contains ddl statement.
    - configure_mappers() should be called prior to table creation.
    """

    created_at = Column(
        TIMESTAMP(timezone=True), server_default=utcnow(), nullable=False
    )
    updated_at = Column(
        TIMESTAMP(timezone=True), server_default=utcnow(),
        server_onupdate=FetchedValue(for_update=True), nullable=False
    )

    @classmethod
    def __declare_first__(cls):
        table = getattr(cls, '__table__', None)

        if table is not None:
            event.listen(
                table, 'after_create', CreateUpdateAtTrigger(table.fullname)
            )
            event.listen(
                table, 'before_drop', DropUpdateAtTrigger(table.fullname)
            )
