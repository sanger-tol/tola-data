import json
import pathlib
import sys

import click

from tola.pretty import bold, colour_pager, s
from tola.terminal import TerminalDict


@click.command()
@click.argument(
    "file_list",
    nargs=-1,
    type=click.Path(
        path_type=pathlib.Path,
        exists=True,
        readable=True,
        dir_okay=False,
    ),
)
def view(file_list):
    """
    Use the pager to view ND-JSON data in FILE_LIST or from STDIN using the
    pretty terminal output.
    """

    if file_list:
        fh_list = [x.open() for x in file_list]
    elif not sys.stdin.isatty():
        fh_list = [sys.stdin]
    colour_pager(itr_ndjson_file_handles(fh_list))


def itr_ndjson_file_handles(fh_list):
    row_count = 0
    for fh in fh_list:
        for line in fh:
            row_count += 1
            yield TerminalDict(json.loads(line)).pretty()

    if row_count:
        yield f"\n{bold(row_count)} row{s(row_count)}"
