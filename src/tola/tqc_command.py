import logging
import pathlib
import re
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
    help=(
        "Column name use to uniquely identify rows."
        " Defaults to the json:api `id` column"
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
@file
@id_list
def edit_col(client, table, key, id_list, file_list):
    """Show or set the value of a column for a list of IDs.

    ID_LIST is a list of IDs to operate on, which can, alternatively, be
    provided on STDIN or in --file arguments.
    """
    logging.debug(f"Client for {client.tolqc_url}")
    logging.debug(f"{file_list = }")

    # Get key for table from ads

    for id_ in build_id_iterator(key, id_list, file_list):
        logging.debug(f"id = {id_}")


@cli.command
@click.pass_obj
@click.option(
    "--apply/--dry",
    "apply_flag",
    default=False,
    show_default=True,
    help="Apply changes or perform a dry run and show changes which would be made.",
)
@table
@key
@file
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


def build_id_iterator(key, id_list, file_list):
    if id_list:
        for id_ in id_list:
            yield id_

    if file_list:
        for file in file_list:
            extn = file.suffix.lower()
            if extn == ".ndjson":
                if not key:
                    sys.exit(
                        "Missing --key argument required"
                        f" to extract ID field from {file}"
                    )
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
