import click

from sqlalchemy import MetaData
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import configure_mappers
from sqlalchemy.schema import DropTable

import tolqc.assembly_models
import tolqc.sample_data_models
import tolqc.system_models

from tolqc.model import Base

from tola import db_connection


@compiles(DropTable, "postgresql")
def compile_drop_table(element, compiler, **kw):
    """Add CASCADE to PostgreSQL DROP TABLE statements

    PostgreSQL will refuse to drop tables which contain dependent FOREIGN KEYs,
    but adding a CASCADE automatically removes them from the other table.
    """
    return compiler.visit_drop_table(element, **kw) + " CASCADE"


@click.command(
    help="Create TolQC database tables, but preserve user, auth and role tables"
)
@db_connection.tolqc_db
@click.option(
    "--drop-tables/--patch",
    help="Drop and recreate (empty) tables, or just patch in missing tables",
    default=False,
    show_default=True,
)
def main(tolqc_db, drop_tables):
    engine, Session = tolqc_db

    if drop_tables:
        old_db = MetaData()
        old_db.reflect(engine)

        keep_tables = {
            "alembic_version",
            "centre",
            "project",
            "token",
            "user",
        }
        drop_tables = []
        for name in old_db.tables:
            if (name in keep_tables) or (name.endswith("_dict")):
                continue
            drop_tables.append(old_db.tables[name])
        old_db.drop_all(engine, tables=drop_tables)

    configure_mappers()
    Base.metadata.create_all(engine)


if __name__ == "__main__":
    main()
