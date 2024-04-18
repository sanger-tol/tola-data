import logging
import pathlib
import sys

import click

from tol.core import DataSourceFilter

from tola import tolqc_client

table = click.option(
    "--table",
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
    help="Input files.",
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
def col(client, table, key, id_list, file_list):
    """Show or set the value of a column for a list of IDs

    ID_LIST is a list of IDs to operate on, which can, alternatively, be
    provided on STDIN or in --file arguments.
    """
    logging.debug(f"Client for {client.tolqc_url}")


@cli.command
@click.pass_obj
@table
@key
@file
def row(client, table, key, file_list):
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
