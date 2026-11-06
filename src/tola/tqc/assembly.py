import logging
import sys
from collections.abc import Iterable
from pathlib import Path

import click

from tola import click_options
from tola.assembly.set import AssemblySetHandler
from tola.assembly.updater import AssemblyUpdater
from tola.ndjson import ndjson_row
from tola.pretty import plain_text_from_itr
from tola.terminal import colour_pager, pretty_dict_itr
from tola.tolqc_client import TolClient
from tola.tqc.engine import id_iterator, input_objects_or_exit

log = logging.getLogger(__name__)


@click.group
def assembly():
    """
    Create, update and show assemblies.
    """

def render_output(pretty: bool, header: str, output_obj: list[dict]) -> None:
    isa_tty = sys.stdout.isatty()

    if pretty or isa_tty:
        itr = pretty_dict_itr(output_obj, "name", head=header)
        if isa_tty:
            colour_pager(itr)
        else:
            sys.stdout.write(plain_text_from_itr(itr))
    else:
        for obj in output_obj:
            sys.stdout.write(ndjson_row(obj))


@assembly.command
@click.pass_context
@click.option(
    "--key",
    type=click.Choice(
        ["assembly.id", "name", "specimen.id"],
        case_sensitive=True,
    ),
    default="assembly.id",
    show_default=True,
    help="Attribute to use to search for assembly sets.",
)
@click_options.file
@click_options.file_format
@click_options.id_list
@click_options.pretty
def show_set(
    ctx,
    key: str,
    file_list: list[Path],
    file_format: str,
    id_list: list[str],
    pretty: bool,
) -> None:
    """
    Show assemblies sets in the format used for the "set-load" subcommand.
    """
    client: TolClient = ctx.obj

    asm_id_itr = id_iterator(
        key,
        id_list=id_list,
        file_list=file_list,
        file_format=file_format,
    )

    hdlr = AssemblySetHandler(client)
    output_obj = []
    for asm_id in asm_id_itr:
        output_obj.extend(hdlr.fetch_assembly_sets(key, asm_id))
    if output_obj:
        render_output(pretty, "Found {} assembly set{}:", output_obj)


@assembly.command
@click.pass_context
@click_options.file
@click_options.file_format
@click_options.id_list
@click_options.pretty
def load_set(
    ctx,
    file_list: list[Path],
    pretty: bool,
) -> None:
    """
    Either load new assembly sets and "release" assembly table component
    entries and set their statuses, or update existing sets and components
    where "assembly.id" is set.
    """
    client: TolClient = ctx.obj

    input_obj = input_objects_or_exit(ctx, file_list)
    asm_ldr = AssemblySetHandler(client)
    output_obj = []
    for obj in input_obj:
        output_obj.append(asm_ldr.load_submitted_set(obj))
    render_output(pretty, "Loaded or updated {} assembly set{}:", output_obj)


@assembly.command
@click.pass_context
@click_options.file
@click_options.file_format
@click_options.id_list
@click_options.pretty
def public_to_downloaded(
    ctx,
    file_list: list[Path],
    file_format: str,
    id_list: list[str],
    pretty: bool,
) -> None:
    """
    Set assemblies with "ENA Public" status to "Downloaded" using the
    timestamp of the expected assembly file on the filesystem for the status
    time where the file is present.  By default queries the database for
    all "ENA Public" assemblies, or acts on a list of "assembly.id" provided
    in the input files or ID_LIST.
    """
    client: TolClient = ctx.obj

    asm_ids = tuple(
        id_iterator(
            "assembly.id",
            id_list=id_list,
            file_list=file_list,
            file_format=file_format,
        )
    )
    asm_updtr = AssemblyUpdater(client)
    if asm_ids:
        asm_updtr.update_to_downloaded(asm_ids)
    else:
        asm_updtr.update_ena_public_to_downloaded()
    asm_updtr.report_update_counts(pretty, sys.stdout)


@assembly.command
@click.pass_context
@click_options.file
@click_options.file_format
@click_options.id_list
@click_options.pretty
def update_downloaded_time(
    ctx,
    file_list: list[Path],
    file_format: str,
    id_list: list[str],
    pretty: bool,
) -> None:
    """
    Set the status time of "Downloaded" assemblies to the timestamp of the
    assembly file on the filesystem.  Acts on the "assembly.id" provided in
    the input files or ID_LIST.
    """
    client: TolClient = ctx.obj

    asm_ids = tuple(
        id_iterator(
            "assembly.id",
            id_list=id_list,
            file_list=file_list,
            file_format=file_format,
        )
    )
    asm_updtr = AssemblyUpdater(client)
    asm_updtr.update_downloaded_status_time(asm_ids)
    asm_updtr.report_update_counts(pretty, sys.stdout)
