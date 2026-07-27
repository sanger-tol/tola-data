import logging
import sys

import click

from tola import click_options
from tola.ndjson import ndjson_row

log = logging.getLogger(__name__)


@click.command()
@click.pass_context
@click.option(
    "--auto/--report",
    "auto_update",
    help="""
      Auto update the MLWH with any `data` table files where `accession_id`
      is "Request" or report to STDOUT
    """,
    default=True,
    show_default=True,
)
@click_options.file
@click_options.file_format
@click.argument(
    "data-id-list",
    nargs=-1,
    required=False,
)
def mlwh_ena(ctx, auto_update, file_list, file_format, data_id_list):
    """
    Add raw data entries for submission to the ENA, to create run accessions.
    Creates or updates entries in the MLWH `tol_sample_bioproject` table used
    to populate the SubTrack ENA submissions system.

    Defaults to any `data` table entries where `accession.id` is "Request", or
    for each `data.id` given in DATA_ID_LIST, or for each `data.id` values in
    each row in each "--file" argument given.
    """

    client = ctx.obj
    for file in client.ads_get_list(
        "file",
        {
            "data.accession.id": {
                "eq": {"value": "Request"},
            },
            "file_type": {
                "in_list": {"value": ["BAM", "CRAM", "RECALL_BAM"]},
            },
        },
        requested_fields=[
            "data.id",
            "data.library.library_type",
            "data.run.platform",
            "data.sample.specimen.species",
        ],
    ):
        row = build_tol_bioproject_row(file)
        sys.stdout.write(ndjson_row(row))


def build_tol_bioproject_row(file):
    data = file.data

    # Check that all the required values are present
    if not (lib := data.library):
        log.warning(f"No library attached to data.id = {data.id!r}")
        return

    if not (run := data.run):
        log.warning(f"No run attached to data.id = {data.id!r}")
        return

    if not (platform := run.platform):
        log.warning(f"No run attached to data.id = {data.id!r} run.id = {run.id!r}")
        return

    if not (lib_type := lib.library_type):
        log.warning(
            f"No library_type attached to data.id = {data.id!r} library.id = {lib.id!r}"
        )
        return

    if not (sample := data.sample):
        log.warning("No sample attached to data.id = {data.id!r}")
        return

    if not (specimen := sample.specimen):
        log.warning(
            f"No specimen attached to data.id = {data.id!r} sample.id = {sample.id!r}"
        )
        return

    if not (species := specimen.species):
        log.warning(
            "No species attached to "
            f"data.id = {data.id!r} specimen.id = {specimen.id!r}"
        )
        return

    # Cannot proceed without an iRODS file name, since iRODS is where
    # DataHose read files from.
    if file.remote_path is None or not file.remote_path.startswith("irods:"):
        return

    return {
        "data_id": data.id,
        "file": file.remote_path[6:],
        "filename": file.name,
        "platform": platform.name,
        "instrument": platform.model,
        "library_name": lib.id,
        "library_source": lib_type.source,
        "library_selection": lib_type.selection,
        "library_strategy": lib_type.strategy,
        "library_type": lib_type.id,
        "library_construction_protocol": lib_type.id,
        # "design_description": build_description(
        #     lib_type.description_template, lib.description
        # ),
        "tolid": specimen.id,
        "biosample_accession": sample.accession.id,
        "bioproject_accession": species.data_accession.id,
    }


def build_description(type_template, lib_desc):
    return f"{type_template} {lib_desc}"
