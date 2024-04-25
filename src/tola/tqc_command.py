import io
import logging
import pathlib
import sys

import click

from tol.core import DataSourceFilter

from tola import tolqc_client
from tola.ndjson import parse_ndjson_stream

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
    ctx.obj = tolqc_client.TolClient(tolqc_url, api_token, tolqc_alias)


@cli.command
@click.pass_obj
@table
@key
@id_list
@file
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
    client, table, key, id_list, file_list, column_name, set_value, apply_flag
):
    """Show or set the value of a column for a list of IDs.

    ID_LIST is a list of IDs to operate on, which can, alternatively, be
    provided on STDIN or in --file arguments.
    """

    id_list = tuple(id_iterator(key, id_list, file_list))
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
            if val != py_value:
                updates.append(
                    ObjFactory(table, id_=obj.id, attributes={column_name: py_value})
                )
                msg.write(f"{null_if_none(val)}:{set_value}\t{oid}\n")
        ads.upsert(table, updates)
        click.echo(msg.getvalue(), nl=False)
    else:
        click.echo(f"{table}.{column_name}\t{key}")
        for obj in fetched:
            val = obj.attributes.get(column_name)
            oid = getattr(obj, key)
            click.echo(f"{null_if_none(val)}\t{oid}")


@cli.command
@click.pass_obj
@table
@key
@file
@apply_flag
def edit_rows(client, table, key, file_list, apply_flag):
    """Populate or update rows in a table from ND-JSON input"""
    pass


@cli.command
@click.pass_obj
@table
@key
@file
@id_list
def show(client, table, key, file_list, id_list):
    """Show rows from a table in the ToLQC database"""
    logging.debug(f"{file_list = }")


def null_if_none(val):
    return "null" if val is None else val


def fetch_all(client, table, key, id_list):
    filt = DataSourceFilter(in_list={key: id_list})
    fetched_data = {
        getattr(x, key): x for x in client.ads.get_list(table, object_filters=filt)
    }

    # Check if we found a data record for each name_root
    if missed := set(id_list) - fetched_data.keys():
        sys.exit(f"Error: Failed to fetch data records named: {sorted(missed)}")

    return [fetched_data[x] for x in id_list]


def id_iterator(key, id_list, file_list):
    if id_list:
        for id_ in id_list:
            yield id_

    if file_list:
        for file in file_list:
            extn = file.suffix.lower()
            if extn == ".ndjson":
                for id_ in ids_from_ndjson_file(key, file):
                    yield id_
            else:
                for id_ in parse_id_list_file(file):
                    yield id_


def parse_id_list_file(file):
    for line in file.open():
        yield line.strip()


def ids_from_ndjson_file(key, file):
    for row in parse_ndjson_stream(file.open()):
        id_ = row[key]
        if id_ != None:
            yield id_
