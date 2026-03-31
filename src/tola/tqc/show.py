import contextlib
import sys

import click
from tol.core import ReqFieldsTree

from tola import click_options
from tola.ndjson import ndjson_row
from tola.pretty import bold, italic, s
from tola.terminal import TerminalDict, close_pager, open_pager
from tola.tqc.async_pager import AsyncQueryPager
from tola.tqc.engine import (
    async_fetch_all_itr,
    comma_split_list,
    core_data_object_to_dict,
    id_iterator,
)
from tola.tqc.query_parser import QueryParser


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
@click.option(
    "--fields",
    "-f",
    "fields_txt",
    multiple=True,
    help=f"""
      Fields to request from the server, which may be in separate objects.
      {italic("e.g.")} `files.md5` would fetch a list of `md5` from the
      `files` to-many relationship.
    """,
)
@click.option(
    "--query",
    "-q",
    "queries_txt",
    multiple=True,
    help=f"""
      Filters to apply when querying the database. {italic("e.g.")}
      `sample.specimen.species.id='Vulpes vulpes'` when showing rows from the
      `data` table will return rows where the `sample` to-one relation links
      to a `specimen` and the `specimen` links to the red fox `species`.
    """,
)
@click_options.id_list
def show(
    client,
    table,
    key,
    file_list,
    file_format,
    show_modified,
    fields_txt,
    queries_txt,
    id_list,
):
    """Show rows from a table in the ToLQC database

    ID_LIST is a list of IDs to operate on, which can additionally be provided
    in --file arugments (in ND-JSON or TXT format), or alternatively piped to
    STDIN.

    Output is in human readable format if STOUT is a terminal, or ND-JSON if
    redirected to a file or UNIX pipe.
    """

    fields = comma_split_list(fields_txt)
    if show_modified:
        fields.append("modified_user")
    req_fields_tree = None
    if fields:
        req_fields_tree = ReqFieldsTree(
            object_type=table,
            data_source=client.ads_ro,
            requested_fields=fields,
        )

    filter_dict = None
    if queries_txt:
        filter_dict = QueryParser(queries_txt).filter_dict()

    if key == "id":
        key = f"{table}.id"

    id_list = tuple(id_iterator(key, id_list, file_list, file_format))

    print_item, pager = build_printer(key, req_fields_tree, show_modified)

    query = AsyncQueryPager(
        query_itr=async_fetch_all_itr(
            client,
            table,
            key,
            id_list,
            requested_tree=req_fields_tree,
            filter_dict=filter_dict,
        ),
        output=print_item,
        queue_size=5 * client.page_size,
    )
    row_count = query.run()

    if pager:
        with contextlib.suppress(OSError, KeyboardInterrupt):
            pager.stdin.write(f"\nFound {bold(row_count)} row{s(row_count)}\n")
        close_pager(pager)


def build_printer(key, req_fields_tree=None, show_modified=False):
    if sys.stdout.isatty():
        pager = open_pager()

        def pretty_cdo_printer(cdo):
            text = TerminalDict(
                core_data_object_to_dict(
                    cdo, requested_tree=req_fields_tree, show_modified=show_modified
                ),
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
            ndjson_row(
                core_data_object_to_dict(
                    cdo, requested_tree=req_fields_tree, show_modified=show_modified
                )
            )
        )
        return True

    return ndjson_row_printer, None
