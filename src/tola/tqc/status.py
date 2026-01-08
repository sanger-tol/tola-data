import sys

import click

from tola import click_options
from tola.ndjson import ndjson_row
from tola.pretty import bold
from tola.terminal import TerminalDict, colour_pager
from tola.tqc.engine import (
    input_objects_or_exit,
    parse_datetime_fields,
)


@click.command()
@click.pass_context
@click_options.table
@click_options.input_files
def status(ctx, table, input_files):
    """
    Store new current statuses for specimens, datasets or assemblies. The
    format of each line of the ND-JSON input should be:

      {"<table>.id": <str>, "status_type.id": <str>, "status_time": <datetime>}

    where "status_type.id" is the name of the status to be set. If this status
    is already set, a new status will not be created, and the current status
    will be returned showing the time set on the server.

    The "status_time" field is optional and will be set to the current time on
    the server if omitted. If provided it should be an ISO-8601 format
    string, which can be the date without the time portion.
    """

    client = ctx.obj

    input_obj = input_objects_or_exit(ctx, input_files)
    parse_datetime_fields(("status_time",), input_obj)
    stored_status = client.ndjson_post(
        f"loader/status/{table}", (ndjson_row(x) for x in input_obj)
    )

    if stored_status:
        if sys.stdout.isatty():
            colour_pager(pretty_status_itr(stored_status, table))
        else:
            print_statuses_in_input_order(table, input_obj, stored_status)


def print_statuses_in_input_order(table, input_obj, stored_status):
    # Build a dict storing statuses by parent object ID
    key = f"{table}.id"
    id_status = {}
    for st_list in stored_status.values():
        for st in st_list:
            id_status[st[key]] = st

    # Write out the statuses in the same order they were input
    for st_id in (x[key] for x in input_obj):
        if st := id_status.get(st_id):
            sys.stdout.write(ndjson_row(st))
        else:
            sys.exit(f"Error: Failed to find status for {st_id!r} in server response")


def pretty_status_itr(stored_status, table):
    for label in sorted(stored_status):
        stored = stored_status[label]
        plural = "" if len(stored) == 1 else "es"
        max_hdr = max(len(x) for x in stored[0])
        yield f"\n{bold(len(stored))} {label} {table} status{plural}\n"
        for st in stored:
            yield TerminalDict(st, max_hdr).pretty()
