import click
import pathlib
import sys

import tola.marshals
from tola import db_connection
from tolqc.model import Base


@click.command(help="Populate dictionary table in ToL QC db from TSV files")
@tola.marshals.mrshl
@click.argument(
    "TSV_FILE",
    nargs=-1,
    type=click.Path(
        dir_okay=False,
        exists=True,
        readable=True,
        path_type=pathlib.Path,
    ),
)
def main(mrshl, tsv_file):

    table_class = table_name_to_class()

    for dict_file in tsv_file:
        table_name = dict_file.stem
        cls = table_class[table_name]
        for spec in read_tsv(dict_file):
            mrshl.update_or_create(cls, spec)

    mrshl.commit()


def table_name_to_class():
    return {
        cls.__tablename__: cls
        for cls in (mppr.class_ for mppr in Base.registry.mappers)
    }


def read_tsv(path):
    with path.open() as fh:
        header_line = split_tsv_line(fh.readline())
        for line in fh:
            cols = split_tsv_line(line)
            spec = {}
            for attr, val in zip(header_line, cols):
                spec[attr] = val
            yield spec


def split_tsv_line(line):
    return list(None if x == r"\N" else x for x in line.rstrip("\r\n").split("\t"))


if __name__ == "__main__":
    main(sys.argv[1:])
