import logging
import sys

import click
from requests.exceptions import HTTPError
from tol.core.datasource_error import DataSourceError

from tola import click_options, tolqc_client
from tola.db_connection import ConnectionParamsError
from tola.pretty import bold, setup_pager
from tola.tqc.add import add
from tola.tqc.dataset import dataset
from tola.tqc.delete import delete
from tola.tqc.edit import edit_col, edit_rows_cli
from tola.tqc.genomescope import genomescope
from tola.tqc.rename import rename
from tola.tqc.report import report
from tola.tqc.show import show
from tola.tqc.species import upsert_species
from tola.tqc.status import status
from tola.tqc.subtrack import subtrack
from tola.tqc.view import view


def cli():
    try:
        tqc_main()
    except DataSourceError as dse:
        sys.exit(f"{dse.status_code}: {dse.title} - {dse.detail}")
    except (HTTPError, ValueError) as err:
        sys.exit(f"{err.__class__.__name__}: {err}")


@click.group
@click_options.tolqc_alias
@click_options.tolqc_url
@click_options.api_token
@click_options.log_level
@click.pass_context
def tqc_main(ctx, tolqc_alias, tolqc_url, api_token, log_level):
    """Show and update rows and columns in the ToLQC database"""
    logging.basicConfig(level=getattr(logging, log_level))
    setup_pager()
    try:
        ctx.obj = tolqc_client.TolClient(tolqc_url, api_token, tolqc_alias)
    except ConnectionParamsError as cpe:
        if sys.stdout.isatty():
            # Show help if we're on a TTY
            err = "Error: " + bold("\n".join(cpe.args))
            sys.exit(ctx.get_help() + "\n\n" + err)
        else:
            sys.exit("\n".join(cpe.args))


tqc_main.add_command(add)
tqc_main.add_command(dataset)
tqc_main.add_command(delete)
tqc_main.add_command(edit_col)
tqc_main.add_command(edit_rows_cli, name="edit-rows")
tqc_main.add_command(genomescope)
tqc_main.add_command(rename)
tqc_main.add_command(report)
tqc_main.add_command(show)
tqc_main.add_command(upsert_species, name="species")
tqc_main.add_command(status)
tqc_main.add_command(subtrack)
tqc_main.add_command(view)
