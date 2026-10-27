import logging
import sys
from pathlib import Path

import click

from tola import click_options
from tola.assembly.updater import AssemblyUpdater
from tola.tolqc_client import TolClient
from tola.tqc.engine import id_iterator

log = logging.getLogger(__name__)


@click.command
@click.pass_context
@click.option(
    "--load-release",
    flag_value=True,
    help="""
      Load new assembly sets and "release" assembly table entries, setting
      their status to "Submitted".
    """,
)
@click.option(
    "--public-to-downloaded",
    flag_value=True,
    help="""
      Set assemblies with "ENA Public" status to "Downloaded" using the
      timestamp of the expected assembly file on the filesystem for the
      status time where the file is present.  By default queries the database
      for all "ENA Public" assemblies, or acts on a list of "assembly.id"
      provided in the input files or ID_LIST.
    """,
)
@click.option(
    "--update-downloaded-time",
    flag_value=True,
    help="""
      Set the status time of "Downloaded" assemblies to the timestamp of the
      assembly file on the filesystem.  Acts on the "assembly.id" provided in
      the input files or ID_LIST.
    """,
)
@click_options.file
@click_options.file_format
@click_options.id_list
@click_options.pretty
def assembly(
    ctx,
    load_release: bool,
    public_to_downloaded: bool,
    update_downloaded_time: bool,
    file_list: list[Path],
    file_format: str,
    id_list: list[str],
    pretty: bool,
) -> None:
    """
    Update "ENA Public" assemblies to "Downloaded" or patch the time
    of "Downloaded" assembly statuses to the timestamp of the assembly file
    on the filesystem.
    """
    client: TolClient = ctx.obj

    if public_to_downloaded or update_downloaded_time:
        asm_ids = tuple(
            id_iterator(
                "assembly.id",
                id_list=id_list,
                file_list=file_list,
                file_format=file_format,
            )
        )
        asm_updtr = AssemblyUpdater(client)
        if public_to_downloaded:
            if asm_ids:
                asm_updtr.update_to_downloaded(asm_ids)
            else:
                asm_updtr.update_ena_public_to_downloaded()
        else:
            asm_updtr.update_downloaded_status_time(asm_ids)

        asm_updtr.report_update_counts(pretty, sys.stdout)
    elif load_release:
        raise NotImplementedError()
    else:
        sys.exit(ctx.get_help())
