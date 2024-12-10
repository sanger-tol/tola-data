import logging
import sys

import click

from tola import click_options, tolqc_client
from tola.db_connection import ConnectionParamsException
from tola.pretty import bold, setup_pager
from tola.tqc.add import add
from tola.tqc.dataset import dataset
from tola.tqc.delete import delete
from tola.tqc.edit import edit_col, edit_rows
from tola.tqc.show import show
from tola.tqc.status import status


@click.group()
@click_options.tolqc_alias
@click_options.tolqc_url
@click_options.api_token
@click_options.log_level
@click.pass_context
def cli(ctx, tolqc_alias, tolqc_url, api_token, log_level):
    """Show and update rows and columns in the ToLQC database"""
    logging.basicConfig(level=getattr(logging, log_level))
    setup_pager()
    try:
        ctx.obj = tolqc_client.TolClient(
            tolqc_url, api_token, tolqc_alias, page_size=100
        )
    except ConnectionParamsException as cpe:
        if sys.stdout.isatty():
            # Show help if we're on a TTY
            err = "Error: " + bold("\n".join(cpe.args))
            sys.exit(ctx.get_help() + "\n\n" + err)
        else:
            sys.exit("\n".join(cpe.args))


cli.add_command(add)
cli.add_command(dataset)
cli.add_command(delete)
cli.add_command(edit_col)
cli.add_command(edit_rows)
cli.add_command(show)
cli.add_command(status)
