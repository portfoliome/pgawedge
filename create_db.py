import os
import sys

import psycopg2
import sqlalchemy as sa
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def main():
    database = 'postgres'
    user = 'postgres'

    url = sa.engine.url.URL('postgresql', host=os.environ['PGHOST'],
                            database=database, username=user)
    ddl_text = sa.text('CREATE DATABASE {};'.format(os.environ['PGDATABASE']))
    engine = sa.create_engine(url)
    engine.raw_connection().set_isolation_level(
        ISOLATION_LEVEL_AUTOCOMMIT
    )

    try:
        engine.execute(ddl_text)
        sys.stdout.write('Creating environment successfully.\n')
    except psycopg2.Error:
        raise SystemExit('Could not connect to PostgreSQL.\n{0}'.format(sys.exc_info()))


if __name__ == '__main__':
    main()
