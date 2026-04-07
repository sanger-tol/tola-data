import contextlib
import sys

import click

from tola import click_options
from tola.ndjson import ndjson_row
from tola.pretty import bold, italic, s
from tola.terminal import TerminalDict, close_pager, open_pager
from tola.tolqc_client import TolClient
from tola.tqc.async_pager import AsyncQueryPager
from tola.tqc.engine import (
    async_fetch_all_itr,
    comma_split_list,
    id_iterator,
    req_fields_tree_cdo_to_dict,
)
from tola.tqc.query_parser import QueryParser


@click.command()
@click.pass_obj
@click_options.table
@click_options.key
@click_options.file
@click_options.file_format
@click.option(
    "--fields",
    "-f",
    "fields_txt",
    multiple=True,
    metavar="<FIELD_PATH>",
    help=f"""
      Fields to request from the server. Fields in related objects can be
      requested using a "." separated list of relationship names followed by
      the field.  {italic("e.g.")} "files.md5" would fetch a list of "md5"
      from the "files" to-many relationship.  Can be given muliple times and
      as comma separated lists.  A path ending in ".id" ensures that only
      the ".id" field from that object is fetched if no other fields in the
      object are requested.
    """,
)
@click.option(
    "--query",
    "-q",
    "queries_txt",
    multiple=True,
    metavar="<FIELD_PATH><OPERATOR><VALUE>",
    help=f"""
      Filters to apply when querying the database. {italic("e.g.")}
      "sample.specimen.species.id='Vulpes vulpes'" when showing rows from the
      "data" table will return rows where the "sample" to-one relation links
      to a "specimen" and the "specimen" links to that "species" of fox.
      Multiple query terms can be given, which are combined by "AND".

      \b
      Available operators:
        =   equal (can use "null" as the VALUE)
        !=  not-equal
        <   less than
        <=  less than or equal
        >   greater than
        >=  greater than or equal
        %   contains (case insensitive sub-string match)
        !%  does not contain

      Query filters containing "<" or ">" require enclosing the query term in
      quotes to avoid shell redirection.
    """,
)
@click.option(
    "--modified",
    "-m",
    "show_modified",
    flag_value=True,
    default=False,
    help="For LogBase derived tables, show who last modified each row and when.",
)
@click_options.id_list
def show(
    client: TolClient,
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
    if not fields:
        fields = None

    req_fields_tree = client.build_req_fields_tree(
        table,
        requested_fields=fields,
    )

    # The filled `ReqFieldsTree` enumerates all of the attributes so that the
    # output JSON can have the same shape on all rows even where related
    # objects are missing.
    filled_tree = client.build_req_fields_tree(
        table,
        requested_fields=fields,
        all_attributes=True,
    )

    filter_dict = None
    if queries_txt:
        filter_dict = QueryParser(queries_txt).filter_dict()

    if key == "id":
        key = f"{table}.id"

    id_list = tuple(id_iterator(key, id_list, file_list, file_format))

    print_item, pager = build_printer(key, filled_tree)

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


def build_printer(key, req_fields_tree):
    if sys.stdout.isatty():
        pager = open_pager()

        def pretty_cdo_printer(cdo):
            text = TerminalDict(
                req_fields_tree_cdo_to_dict(req_fields_tree, cdo),
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
        sys.stdout.write(ndjson_row(req_fields_tree_cdo_to_dict(req_fields_tree, cdo)))
        return True

    return ndjson_row_printer, None
