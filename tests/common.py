from unittest.util import safe_repr
from functools import wraps

PG_UPSERT_VERSION = (9, 5)


class PostgresStatementFixture(object):
    maxDiff = True

    def assertSQLStatementEqual(self, first, second, msg=None):
        if squeeze_whitespace(first) != squeeze_whitespace(second):
            standardMsg = 'SQL statement {0} != {1}'.format(
                safe_repr(first), safe_repr(second))
            self.fail(self._formatMessage(msg, standardMsg))


def skipPGVersionBefore(*ver):
    """Skip PG versions below specific version i.e. (9, 5)."""

    ver = ver + (0,) * (3 - len(ver))

    def skip_before_postgres_(func):
        @wraps(func)
        def skip_before_postgres__(obj, *args, **kwargs):

            if hasattr(obj.conn, 'server_version'):
                server_version = obj.conn.server_version
            else:  # Assume Sqlalchemy
                server_version = obj.conn.connection.connection.server_version

            if server_version < int('%d%02d%02d' % ver):
                return obj.skipTest("Skipped because PostgreSQL {}".format(
                    server_version))
            else:
                return func(obj, *args, **kwargs)
        return skip_before_postgres__
    return skip_before_postgres_


def squeeze_whitespace(text):
    """Remove extra whitespace, newline and tab characters from text."""

    return ' '.join(text.split())
