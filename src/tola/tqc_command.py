import io
import logging
import pathlib
import sys

import click

from tol.core import DataSourceFilter

from tola import tolqc_client
from tola.db_connection import ConnectionParamsException
from tola.ndjson import ndjson_row, parse_ndjson_stream

table = click.option(
    "--table",
    required=True,
    help="Name of table in ToLQC database",
)

key = click.option(
    "--key",
    default="id",
    show_default=True,
    help=(
        "Column name use to uniquely identify rows."
        # " Defaults to the json:api `id` column"
    ),
)

file = click.option(
    "--file",
    "file_list",
    type=click.Path(
        path_type=pathlib.Path,
        exists=True,
        readable=True,
    ),
    multiple=True,
    help="Input file names.",
)

id_list = click.argument(
    "id_list",
    nargs=-1,
    required=False,
)

apply_flag = click.option(
    "--apply/--dry",
    "apply_flag",
    default=False,
    show_default=True,
    help="Apply changes or perform a dry run and show changes which would be made.",
)


@click.group()
@tolqc_client.tolqc_alias
@tolqc_client.tolqc_url
@tolqc_client.api_token
@tolqc_client.log_level
@click.pass_context
def cli(ctx, tolqc_alias, tolqc_url, api_token, log_level):
    """Show and update rows and columns in the ToLQC database"""
    logging.basicConfig(level=getattr(logging, log_level))
    try:
        ctx.obj = tolqc_client.TolClient(tolqc_url, api_token, tolqc_alias)
    except ConnectionParamsException as cpe:
        if sys.stdout.isatty():
            # Show help if we're on a TTY
            err = "Error: " + click.style("\n".join(cpe.args), bold=True)
            sys.exit(ctx.get_help() + "\n\n" + err)
        else:
            raise cpe


@cli.command
@click.pass_obj
@table
@key
@id_list
@file
@tolqc_client.file_format
@click.option(
    "--column-name",
    "--col",
    help="Name of column to edit",
    required=True,
)
@click.option(
    "--set-value",
    "--set",
    help="Value to set column to",
)
@apply_flag
def edit_col(
    client,
    table,
    key,
    id_list,
    file_list,
    file_format,
    column_name,
    set_value,
    apply_flag,
):
    """Show or set the value of a column for a list of IDs.

    ID_LIST is a list of IDs to operate on, which can additionally be provided
    in --file arugments, or alternatively piped to STDIN.
    """

    id_list = tuple(id_iterator(key, id_list, file_list, file_format))
    fetched = fetch_all(client, table, key, id_list)

    ads = client.ads
    ObjFactory = ads.data_object_factory
    if set_value:
        py_value = None if set_value == "null" else set_value
        updates = []
        msg = io.StringIO()
        for obj in fetched:
            val = getattr(obj, column_name)
            logging.debug(f"{obj.attributes}")
            oid = getattr(obj, key)

            # Would value be changed?
            if val != py_value:
                updates.append(
                    ObjFactory(table, id_=obj.id, attributes={column_name: py_value})
                )
                msg.write(f"{null_if_none(val)}\t{set_value}\t{oid}\n")

        if updates:
            if apply_flag:
                ads.upsert(table, updates)
            click.echo(f"previous_value\t{table}.{column_name}\t{key}")
            click.echo(msg.getvalue(), nl=False)
            if not apply_flag:
                click.echo("Dry run. Use '--apply' flag to store changes.", err=True)

    elif fetched:
        click.echo(f"{table}.{column_name}\t{key}")
        for obj in fetched:
            val = obj.attributes.get(column_name)
            oid = getattr(obj, key)
            click.echo(f"{null_if_none(val)}\t{oid}")


@cli.command
@click.pass_context
@table
@key
@apply_flag
@click.argument(
    "input_files",
    nargs=-1,
    required=False,
    type=click.Path(
        path_type=pathlib.Path,
        exists=True,
        readable=True,
    ),
)
def edit_rows(ctx, table, key, apply_flag, input_files):
    """Populate or update rows in a table from ND-JSON input

    INPUT_FILES is a list of files in ND-JSON format. Each row is expected to
    contain a value for the key used to identify each record. Any other
    values given will be used to update columns for the record.
    """

    if not input_files and sys.stdin.isatty():
        err = "Error: " + click.style(
            "Missing INPUT_FILES arguments or STDIN input", bold=True
        )
        sys.exit(ctx.get_help() + "\n\n" + err)

    input_obj = []
    if input_files:
        for file in input_files:
            with file.open() as fh:
                input_obj.extend(parse_ndjson_stream(fh))
    else:
        input_obj.extend(parse_ndjson_stream(sys.stdin))

    client = ctx.obj
    ads = client.ads
    ObjFactory = ads.data_object_factory
    id_list = list(x[key] for x in input_obj)
    db_obj = fetch_all(client, table, key, id_list)
    updates = []
    changes = []
    for inp, obj in zip(input_obj, db_obj, strict=True):
        attr = {}
        chng = {}
        for k, inp_v in inp.items():
            if k == 'id':
                continue
            obj_v = obj.get(k)
            if inp_v != obj_v:
                attr[k] = inp_v
                chng[k] = obj_v, inp_v
        if attr:
            changes.append(chng)
            updates.append(
                ObjFactory(table, id_=obj.id, attributes=attr)
            )
    if updates:
        if apply_flag:
            ads.upsert(table, updates)
        for chng in changes:
            print(ndjson_row(chng))






@cli.command
@click.pass_obj
@table
@key
@file
@tolqc_client.file_format
@id_list
def show(client, table, key, file_list, file_format, id_list):
    """Show rows from a table in the ToLQC database

    ID_LIST is a list of IDs to operate on, which can additionally be provided
    in --file arugments, or alternatively piped to STDIN.
    """

    id_list = tuple(id_iterator(key, id_list, file_list, file_format))
    fetched = fetch_all(client, table, key, id_list)
    for obj in fetched:
        print(obj)
        # print(ndjson_row(obj.attributes), end="")



def null_if_none(val):
    return "null" if val is None else val


def fetch_all(client, table, key, id_list):
    filt = DataSourceFilter(in_list={key: id_list})
    fetched_data = {
        getattr(x, key): x for x in client.ads.get_list(table, object_filters=filt)
    }

    # Check if we found a data record for each name_root
    if missed := set(id_list) - fetched_data.keys():
        sys.exit(
            f"Error: Failed to fetch records for {table}.{key} in: {sorted(missed)}"
        )

    return list(fetched_data[x] for x in id_list)


def id_iterator(key, id_list=None, file_list=None, file_format=None):
    if id_list:
        for oid in id_list:
            yield oid

    if file_list:
        for file in file_list:
            fmt = file_format or guess_file_type(file)
            with file.open() as fh:
                if fmt == "NDJSON":
                    for oid in ids_from_ndjson_stream(key, fh):
                        yield oid
                else:
                    for oid in parse_id_list_stream(fh):
                        yield oid
    elif not (id_list or sys.stdin.isatty()):
        # No IDs or files given on command line, and input is not attached to
        # a terminal, so read from STDIN.
        if file_format and file_format == "NDJSON":
            return ids_from_ndjson_stream(key, sys.stdin)
        else:
            return parse_id_list_stream(sys.stdin)


def guess_file_type(file):
    extn = file.suffix.lower()
    return "NDJSON" if extn == ".ndjson" else "TXT"


def parse_id_list_stream(fh):
    for line in fh:
        yield line.strip()


def ids_from_ndjson_stream(key, fh):
    for row in parse_ndjson_stream(fh):
        oid = row[key]
        if oid is not None:
            yield oid
