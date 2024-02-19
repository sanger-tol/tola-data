# SPDX-FileCopyrightText: 2024 Genome Research Ltd.
#
# SPDX-License-Identifier: MIT

import datetime
import inspect
import io
import pathlib
import re
import subprocess
import sys

import black
import click
from psycopg2.errors import DuplicateDatabase
from sqlalchemy import create_engine, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import selectinload, sessionmaker
from sqlalchemy.orm.exc import DetachedInstanceError
from tolqc.model import Base
from tolqc.sample_data_models import (
    AccessionTypeDict,
    Centre,
    Data,
    LibraryType,
    Platform,
    Project,
    QCDict,
    Run,
    Sample,
    Sex,
    Species,
    Specimen,
    VisibilityDict,
)

data_dir = pathlib.Path()


@click.command(
    help="Dump sample data from the production ToLQC database",
)
@click.option(
    "--db-uri",
    envvar="DB_URI",
    help=(
        "URI of the ToLQC source database."
        " Uses DB_URI environment variable if not specified"
    ),
    required=True,
)
@click.option(
    "--sql-data-file",
    help="File of SQL INSERT statements for test database",
    type=click.Path(
        dir_okay=False,
        readable=True,
        path_type=pathlib.Path,
    ),
    default=data_dir / "test_data.sql",
    required=True,
    show_default=True,
)
@click.option(
    "--build-db-uri",
    envvar="BUILD_DB_URI",
    help=(
        "URI of the a database for building SQL dump which will be created"
        " and dropped."
        " Defaults to the --db-uri with the database name 'test_data_build'"
    ),
)
@click.option(
    "--echo-sql/--no-echo-sql",
    help="Echo SQLAlchemy SQL to STDERR",
    default=False,
    show_default=True,
)
@click.option(
    "--create-db/--no-create-db",
    help="Delete or keep database of sample data after creating SQL dump",
    default=False,
    show_default=True,
)
def cli(db_uri, build_db_uri, sql_data_file, echo_sql, create_db):
    # Fetch sample data
    engine = create_engine(db_uri, echo=echo_sql)
    sample_data = build_sample_data(sessionmaker(bind=engine))
    sys.stdout.write(code_string(sample_data))

    if create_db:
        # Create empty database to receive test data
        build_url = make_build_url(db_uri, build_db_uri)
        create_build_db(build_url)
        build_engine = create_engine(build_url, echo=echo_sql)
        Base.metadata.create_all(build_engine)

        # Populate build database
        populate_database(sessionmaker(bind=build_engine), sample_data)

    # # Clean up build database unless it's wanted
    # if delete_build_db:
    #     build_engine.dispose()
    #     drop_build_db(build_url)


def make_sql_data_file(build_url, sql_data_file):
    with open(sql_data_file, "w") as sql_fh:
        sql_dump = subprocess.run(
            (
                "pg_dump",
                "--data-only",
                # "--rows-per-insert=1000",
                build_url.render_as_string(),
            ),
            stdout=sql_fh,
            check=False,
        )
        sql_dump.check_returncode()


def build_sample_data(ssn_maker):
    fetched = []
    with ssn_maker() as session:
        # Fetch data from all of the dictionary-like tables
        for cls in (
            AccessionTypeDict,
            LibraryType,
            Platform,
            Centre,
            QCDict,
            Sex,
            VisibilityDict,
        ):
            entries = session.scalars(select(cls)).all()
            fetched.extend(entries)

        # Projects required
        lims_ids = 5822, 5901, 6327
        fetched.extend(fetch_projects(session, lims_ids))

        # Fetch data for a list of test species
        species_list = "Juncus effusus", "Brachiomonas submarina"
        fetched.extend(fetch_species_data(session, species_list))
    return fetched


def populate_database(ssn_maker, sample_data):
    with ssn_maker() as session:
        for obj in sample_data:
            session.merge(obj)
        session.commit()


def make_build_url(db_uri, build_db_uri):
    if build_db_uri:
        url = make_url(db_uri)
    else:
        url = make_url(db_uri).set(database="test_data_build")
    return url


def create_build_db(build_url):
    engine = pg_engine(build_url)
    try:
        with engine.begin() as conn:
            conn.execute(text(f"CREATE DATABASE {build_url.database}"))
    except ProgrammingError as e:
        if isinstance(e.orig, DuplicateDatabase):
            url_str = build_url.render_as_string(hide_password=True)
            click.echo(f"Error: database '{url_str}' already exists", err=True)
            sys.exit(1)
        else:
            raise e


def drop_build_db(build_url):
    engine = pg_engine(build_url)
    with engine.begin() as conn:
        conn.execute(text(f"DROP DATABASE {build_url.database}"))


def pg_engine(url):
    pg_url = url.set(database="postgres")
    return create_engine(pg_url, isolation_level="AUTOCOMMIT")


def fetch_projects(session, lims_ids):
    statement = select(Project).where(Project.lims_id.in_(lims_ids))
    return session.scalars(statement).all()


def fetch_species_data(session, species_list):
    """
    Fetches a list of Species from the database, with all their data that
    we're interested in pre-fetched and attached via SELECT IN loads.
    """
    statement = (
        select(Species)
        .where(Species.species_id.in_(species_list))
        # Specify a `selectinload` path to each leaf we want fetched
        .options(selectinload(Species.specimens).selectinload(Specimen.accession))
        .options(
            selectinload(Species.specimens)
            .selectinload(Specimen.samples)
            .selectinload(Sample.accession)
        )
        .options(
            selectinload(Species.specimens)
            .selectinload(Specimen.samples)
            .selectinload(Sample.data)
            .selectinload(Data.project_assn)
        )
        .options(
            selectinload(Species.specimens)
            .selectinload(Specimen.samples)
            .selectinload(Sample.data)
            .selectinload(Data.library)
        )
        .options(
            selectinload(Species.specimens)
            .selectinload(Specimen.samples)
            .selectinload(Sample.data)
            .selectinload(Data.run)
            .selectinload(Run.pacbio_run_metrics)
        )
        .options(
            selectinload(Species.specimens)
            .selectinload(Specimen.samples)
            .selectinload(Sample.data)
            .selectinload(Data.files)
        )
    )
    species_data = session.scalars(statement).all()

    # Check that we found all the requested species
    requested = set(species_list)
    fetched = {s.species_id for s in species_data}
    if miss := requested - fetched:
        msg = f"Failed to fetch species: {miss}"
        raise ValueError(msg)

    return species_data


def sqlalchemy_data_objects_repr(sql_alchemy_data):
    """
    Monkey patch in our `__repr__` for SQLAlchemy data objects, get a formatted
    string of the data objects, then restore the original `__repr__`.
    """
    class_list = set()

    def new_repr(self):
        """
        Replacement for SQLAlchemy's `__repr__` which produces valid python
        code for building data objects.
        """
        class_name = self.__class__.__name__
        class_list.add(class_name)
        attribs = []
        for col in self.__mapper__.columns:
            name = col.name
            value = getattr(self, name)
            if value is not None:
                if isinstance(value, datetime.datetime):
                    attribs.append(f"{name}='{value.isoformat()}'")
                else:
                    attribs.append(f"{name}={value!r}")
        for name, rel in self.__mapper__.relationships.items():
            try:
                related_objs = getattr(self, name)
            except DetachedInstanceError:
                # Relationships which were not loaded are not followed
                continue
            if not related_objs:
                # Ignore empty lists
                continue
            attribs.append(f"{name}={related_objs}")
        attrib_str = ", ".join(attribs)
        return f"{class_name}({attrib_str})"

    save_repr = Base.__repr__
    Base.__repr__ = new_repr
    code_repr = repr(sql_alchemy_data)
    Base.__repr__ = save_repr
    return code_repr, class_list


def build_black_mode(max_line_length):
    return black.Mode(
        string_normalization=False,
        line_length=max_line_length,
        target_versions={
            black.TargetVersion[f"PY{sys.version_info.major}{sys.version_info.minor}"],
        },
    )


def black_formatted_string_io(code, black_mode):
    blk_fmt = io.StringIO()
    blk_fmt.write(black.format_str(code, mode=black_mode))
    blk_fmt.seek(0)
    return blk_fmt


def code_string(obj, max_line_length=99):
    tolp_black_mode = build_black_mode(max_line_length - 1)
    obj_repr, class_list = sqlalchemy_data_objects_repr(obj)
    class_list_str = ", ".join(sorted(class_list))
    imports_header = f"\nfrom tolqc.sample_data_models import {class_list_str}\n\n"
    blk_fmt = black_formatted_string_io(
        (
            license_string()
            + imports_header
            + "def test_data():\n    return "
            + obj_repr
        ),
        tolp_black_mode,
    )

    wrapped = wrap_lines(blk_fmt, max_line_length)
    return noqa_lines(wrapped, max_line_length).getvalue().rstrip() + "\n"


def noqa_lines(input_io, max_line_length):
    out = io.StringIO()
    for line in input_io:
        noqa = []
        if "{}" in line:
            noqa.append("P103")
        if len(line) > max_line_length:
            noqa.append("E501")
        if noqa:
            noqa_str = ", ".join(noqa)
            line = line.rstrip() + f"  # noqa: {noqa_str}\n"
        out.write(line)
    out.seek(0)
    return out


def wrap_lines(input_io, max_line_length):
    out = io.StringIO()
    for line in input_io:
        if len(line) > max_line_length:
            for wrapper in wrap_strings, wrap_numbers:
                if wrapped_line := wrapper(line.rstrip(), max_line_length):
                    line = wrapped_line
                    break
        out.write(line)
    out.seek(0)
    return out


def wrap_strings(line, max_line_length):
    m = re.fullmatch(r"(\s+)(\w+)='(.+)',", line)
    if not m:
        return None
    prefix, name, string = m.groups()

    # Build array of wrapped lines
    out = [f"{prefix}{name}=("]
    indent = "    "
    build_init = prefix + indent + "'"
    build = build_init
    chunks = split_string(string)
    for cnk in chunks:
        if len(build) + len(cnk) > max_line_length - 2:
            out.append(build + "'")
            build = build_init
        build += cnk
    if build != build_init:
        out.append(build + "'")

    out.append(f"{prefix}),")
    return string_with_newlines(out)


def split_string(string):
    """
    Split string into chunks on punctuation characters. "." is excluded from
    punctuation characters so that numbers containing decimal points are not
    split.
    """
    chunks = [""]
    for i, ele in enumerate(re.split(r"([^\w\.#]+)", string)):
        if i % 2:
            # Begin chunks with punctuation characters. Strings look better
            # beginning with "/" or " " rather being left on the end of the
            # previous line.
            chunks.append(ele)
        else:
            chunks[-1] += ele

    # Avoid having a single, trailing punctuation character in a separate
    # string.
    if len(chunks[-1]) == 1:
        last = chunks.pop()
        chunks[-1] += last

    return chunks


def wrap_numbers(line, _):
    m = re.fullmatch(r"(\s+)(\w+)=([\d\.]+),", line)
    if not m:
        return None
    prefix, name, number = m.groups()
    indent = "    "
    out = [
        f"{prefix}{name}=(",
        f"{prefix}{indent}{number}",
        f"{prefix}),",
    ]
    return string_with_newlines(out)


def string_with_newlines(lines):
    return "".join(list(f"{x}\n" for x in lines))


def license_string():
    this_year = datetime.date.today().year
    return inspect.cleandoc(
        f"""
        # SPDX-FileCopyrightText: {this_year} Genome Research Ltd.
        #
        # SPDX-License-Identifier: MIT
        """,
    )


if __name__ == "__main__":
    cli()
