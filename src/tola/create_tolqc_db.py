import click

from main.model import Base
from sqlalchemy import MetaData
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.schema import DropTable

from tola import db_connection


@compiles(DropTable, "postgresql")
def compile_drop_table(element, compiler, **kw):
    """Add CASCADE to PostgreSQL DROP TABLE statements

    PostgreSQL will refuse to drop tables which contain dependent FOREIGN KEYs,
    but adding a CASCADE automatically removes them from the other table.
    """
    return compiler.visit_drop_table(element, **kw) + " CASCADE"


@click.command(help="Create TolQC database tables, but preserve user, auth and role tables")
@db_connection.tolqc_db
def main(tolqc_db):
    engine, Session = tolqc_db

    old_db = MetaData()
    old_db.reflect(engine)

    keep_tables = {"user", "auth", "role"}
    drop_tables = []
    for name in old_db.tables:
        if name in keep_tables:
            continue
        drop_tables.append(old_db.tables[name])
    old_db.drop_all(engine, tables=drop_tables)

    Base.metadata.create_all(engine)


if __name__ == "__main__":
    main()
