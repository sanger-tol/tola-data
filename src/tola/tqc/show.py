import contextlib
import sys

import click

from tola import click_options
from tola.ndjson import ndjson_row
from tola.pretty import bold, s
from tola.terminal import TerminalDict, close_pager, open_pager
from tola.tqc.async_pager import AsyncQueryPager
from tola.tqc.engine import (
    async_fetch_all_itr,
    core_data_object_to_dict,
    id_iterator,
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

    print_item, pager = build_printer(key, show_modified)

    query = AsyncQueryPager(
        query_itr=async_fetch_all_itr(
            client, table, key, id_list, show_modified=show_modified
        ),
        output=print_item,
        queue_size=5 * client.page_size,
    )
    row_count = query.run()

    if pager:
        with contextlib.suppress(OSError, KeyboardInterrupt):
            pager.stdin.write(f"\nFound {bold(row_count)} row{s(row_count)}\n")
        close_pager(pager)


def build_printer(key, show_modified=False):
    if sys.stdout.isatty():
        pager = open_pager()

        def pretty_cdo_printer(cdo):
            text = TerminalDict(
                core_data_object_to_dict(cdo, show_modified=show_modified),
                key=key,
            ).pretty()
            try:
                pager.stdin.write(text)
            except (OSError, KeyboardInterrupt):
                # Signal that writing has failed, which happens when the user
                # quits `less` before paging through all of the results.
                return False
            return True

        return pretty_cdo_printer, pager

    def ndjson_row_printer(cdo):
        sys.stdout.write(
            ndjson_row(core_data_object_to_dict(cdo, show_modified=show_modified))
        )
        return True

    return ndjson_row_printer, None
