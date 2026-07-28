import logging
import sys
from typing import Any

import click

from tola import click_options
from tola.ndjson import ndjson_row
from tola.pretty import s
from tola.tolqc_client import TolClient

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
    default=False,
    show_default=True,
)
@click.option(
    "--store",
    "store_flag",
    help="""
      Store input MLWH `tol_sample_bioproject` table rows (which can be
      produced by "--report")
    """,
    flag_value=True,
    default=False,
    show_default=True,
)
@click_options.file
@click_options.file_format
@click.argument(
    "data-id-list",
    nargs=-1,
    required=False,
)
def mlwh_ena(ctx, auto_update, store_flag, file_list, file_format, data_id_list):
    """
    Add raw data entries for submission to the ENA, to create run accessions.
    Creates or updates entries in the MLWH `tol_sample_bioproject` table used
    to populate the SubTrack ENA submissions system.

    Defaults to any `data` table entries where `accession.id` is "Request", or
    for each `data.id` given in DATA_ID_LIST, or for each `data.id` values in
    each row in each "--file" argument given.
    """

    filt_spec = build_filter_spec(data_id_list)

    client: TolClient = ctx.obj
    for file in client.ads_get_list(
        "file",
        filter_spec=filt_spec,
        requested_fields=[
            "data.id",
            "data.library.library_type",
            "data.run.platform",
            "data.sample.specimen.species",
        ],
    ):
        if not (row := build_tol_bioproject_row(file)):
            continue
        sys.stdout.write(ndjson_row(row))


def build_filter_spec(data_id_list: list[str]) -> dict[str, dict[str, Any]]:
    spec = {
        "file_type": {
            "in_list": {"value": ["BAM", "CRAM", "RECALL_BAM"]},
        },
    }

    if data_id_list:
        spec["data.id"] = {
            "in_list": {"value": data_id_list},
        }
    else:
        spec["data.accession.id"] = {
            "eq": {"value": "Request"},
        }

    return spec


def build_tol_bioproject_row(file):
    data = file.data

    # Check that all the required values are present
    missing = []

    # Cannot proceed without an iRODS file name, since iRODS is where
    # DataHose read files from.
    if file.remote_path is None or not file.remote_path.startswith("irods:"):
        missing.append(f"No iRODS path for, file.name = {file.name!r}")

    if not (lib := data.library):
        missing.append("No library attached")

    if not (run := data.run):
        missing.append("No run attached")

    if run and not (platform := run.platform):
        missing.append(f"No run attached to run.id = {run.id!r}")

    if lib and not (lib_type := lib.library_type):
        missing.append(f"No library_type attached to library.id = {lib.id!r}")

    if not (sample := data.sample):
        missing.append("No sample attached")

    if sample and not (specimen := sample.specimen):
        missing.append(f"No specimen attached to sample.id = {sample.id!r}")

    if specimen and not (species := specimen.species):
        missing.append(f"No species attached to specimen.id = {specimen.id!r}")

    if sample and not (bsa := sample.accession):
        missing.append(f"No BioSample accession attached to sample.id = {sample.id!r}")

    if species and not (bpa := species.data_accession):
        missing.append(
            f"No BioSpecimen data_accession attached to species.id = {species.id!r}"
        )

    if missing:
        msg = "\n  ".join(
            [f"Missing value{s(missing)} for data.id = {data.id!r}", *missing]
        )
        log.warning(msg)
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
        "biosample_accession": bsa.id,
        "bioproject_accession": bpa.id,
    }


def build_description(type_template, lib_desc):
    return f"{type_template} {lib_desc}"
