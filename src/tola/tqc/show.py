import sys

import click

from tola import click_options
from tola.ndjson import ndjson_row
from tola.pretty import colour_pager
from tola.tqc.engine import (
    core_data_object_to_dict,
    fetch_all,
    id_iterator,
    pretty_cdo_itr,
)


@click.command()
@click.pass_obj
@click_options.table
@click_options.key
@click_options.file
@click_options.file_format
@click.option(
    "--modified/--hide",
    "-m/-M",
    "show_modified",
    default=False,
    show_default=True,
    help="For LogBase derived tables, show who last modified row and when",
)
@click_options.id_list
def show(client, table, key, file_list, file_format, show_modified, id_list):
    """Show rows from a table in the ToLQC database

    ID_LIST is a list of IDs to operate on, which can additionally be provided
    in --file arugments, or alternatively piped to STDIN.

    Output is in human readable format if STOUT is a terminal, or ND-JSON if
    redirected to a file or UNIX pipe.
    """

    if key == "id":
        key = f"{table}.id"

    id_list = tuple(id_iterator(key, id_list, file_list, file_format))
    fetched = fetch_all(client, table, key, id_list)
    if sys.stdout.isatty():
        colour_pager(pretty_cdo_itr(fetched, key, show_modified=show_modified))
    else:
        for cdo in fetched:
            sys.stdout.write(
                ndjson_row(core_data_object_to_dict(cdo, show_modified=show_modified))
            )
