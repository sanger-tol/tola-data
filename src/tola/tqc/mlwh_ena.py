import logging
import sys
from inspect import cleandoc
from typing import Any

import click
import mysql.connector
from mysql.connector.abstracts import MySQLConnectionAbstract
from tol.core.data_object import DataObject

from tola import click_options, db_connection
from tola.ndjson import ndjson_row
from tola.pretty import bold, s
from tola.terminal import colour_pager, pretty_dict_itr
from tola.tolqc_client import TolClient
from tola.tqc.engine import (
    dicts_to_core_data_objects,
    id_iterator,
    input_objects_or_exit,
)

log = logging.getLogger(__name__)


class MlwhEnaDataError(Exception):
    """
    Data error when storing
    """


@click.command()
@click.pass_context
@click.option(
    "--update/--report",
    "update_mlwh",
    help="Update the MLWH or report to STDOUT",
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
def mlwh_ena(ctx, update_mlwh, store_flag, file_list, file_format, data_id_list):
    """
    Add raw data entries for submission to the ENA, to create run accessions.
    Creates or updates entries in the MLWH `tol_sample_bioproject` table used
    to populate the SubTrack ENA submissions system.

    Defaults to any `data` table entries where `accession.id` is "Request", or
    for each `data.id` given in DATA_ID_LIST, or for each `data.id` values in
    each row in each "--file" argument given.
    """

    client: TolClient = ctx.obj

    if store_flag:
        mlwh_rows = input_objects_or_exit(ctx, file_list)
    else:
        data_id_list = list(
            id_iterator("data.id", data_id_list, file_list, file_format)
        )
        filt_spec = build_filter_spec(data_id_list)
        mlwh_rows = fetch_tol_bioproject_rows(client, filt_spec)

    if not mlwh_rows:
        sys.exit(0)

    conn = db_connection.mlwh_rw_db()
    fetch_sample_table_fields(conn, mlwh_rows, client)
    if store_flag or update_mlwh:
        store_tol_bioproject_rows(conn, mlwh_rows)
        update_request_placeholders_to_pending(client, mlwh_rows)

    if sys.stdout.isatty():
        header = format_header(mlwh_rows, store_flag or update_mlwh)
        colour_pager(pretty_dict_itr(mlwh_rows, "data_id", head=header))
    else:
        for row in mlwh_rows:
            sys.stdout.write(ndjson_row(row))


def format_header(mlwh_rows: list[dict[str, Any]], write_flag: bool):
    new = 0
    for row in mlwh_rows:
        if row.get("id_tsb_tmp"):
            new += 1
    upd = len(mlwh_rows) - new
    return (
        f"Stored {bold(new)} new and updated {bold(upd)}"
        if write_flag
        else f"Generated {bold(len(mlwh_rows))}"
    ) + f" MLWH tol_sample_bioproject table row{s(mlwh_rows)}:"


def fetch_tol_bioproject_rows(
    client: TolClient, filt_spec: dict[str, dict[str, Any]]
) -> list[dict[str, str]]:
    mlwh_rows = []
    for file in client.ads_get_list(
        "file",
        filter_spec=filt_spec,
        requested_fields=[
            "name",
            "remote_path",
            "data.id",
            "data.library.library_type",
            "data.run.platform",
            "data.sample.id",
            "data.sample.specimen.id",
            "data.sample.specimen.species.id",
        ],
    ):
        if row := build_tol_bioproject_row(file):
            mlwh_rows.append(row)

    return mlwh_rows


def build_filter_spec(data_id_list: list[str | int]) -> dict[str, dict[str, Any]]:
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


def update_request_placeholders_to_pending(
    client: TolClient, mlwh_rows: list[dict[str, Any]]
):
    """
    For each `data.id` in `mlwh_rows` update any "Request" placeholder
    accessions to "Pending", now that they've been written to the MLWH
    `tol_sample_bioproject` table.
    """
    data_id_list = [x["data_id"] for x in mlwh_rows]
    req_data_id = []
    for page in client.pages(data_id_list):
        for data in client.ads_get_list(
            "data",
            filter_spec={
                "accession.id": {"eq": {"value": "Request"}},
                "id": {"in_list": {"value": page}},
            },
            requested_fields=["id"],  # Fetch the minimum data
        ):
            req_data_id.append(data.id)

    ads = client.ads
    for page in client.pages(req_data_id):
        updates = dicts_to_core_data_objects(
            ads, "data", [{"data.id": x, "accession.id": "Pending"} for x in page]
        )
        ads.upsert("data", updates)


def build_tol_bioproject_row(file: DataObject):
    # Check that all the required values are present
    missing = []

    # Cannot proceed without an iRODS file name, since iRODS is where
    # DataHose read files from.
    if file.remote_path is not None and file.remote_path.startswith("irods:"):
        file_path = file.remote_path[6:]
    else:
        file_path = None
        missing.append(f"No iRODS path for, file.name = {file.name!r}")

    if data := file.data:
        data_id = data.id
    else:
        missing.append(f"No data object attached to file.id = {file.id!r}")
        data_id = None

    if data and not (lib := data.library):
        missing.append("No library attached")

    if data and not (run := data.run):
        missing.append("No run attached")

    if run and not (platform := run.platform):
        missing.append(f"No run attached to run.id = {run.id!r}")

    if lib and not (lib_type := lib.library_type):
        missing.append(f"No library_type attached to library.id = {lib.id!r}")

    if data and not (sample := data.sample):
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
            [f"Missing value{s(missing)} for data.id = {data_id!r}:", *missing]
        )
        log.warning(msg)
        return

    return {
        "data_id": data_id,
        "sample_name": sample.id,
        "file": file_path,
        "filename": file.name,
        "platform": platform.name,
        "instrument": platform.model,
        "library_name": lib.id,
        "library_source": lib_type.source,
        "library_selection": lib_type.selection,
        "library_strategy": lib_type.strategy,
        "library_type": lib_type.id,
        "library_construction_protocol": lib_type.id,
        "design_description": None,
        # "design_description": build_description(
        #     lib_type.description_template, lib.description
        # ),
        "tolid": specimen.id,
        "biosample_accession": bsa.id,
        "bioproject_accession": bpa.id,
    }


def build_description(type_template, lib_desc):
    return f"{type_template} {lib_desc}"


def fetch_sample_table_fields(
    conn: MySQLConnectionAbstract,
    mlwh_rows: list[dict[str, Any]],
    client: TolClient,
) -> None:

    idx: dict[str, list[dict[str, Any]]] = {}
    for row in mlwh_rows:
        idx.setdefault(row["sample_name"], []).append(row)

    crsr = conn.cursor()
    last_page = None
    for page in client.pages(list(idx)):
        page_size = len(page)
        if page_size != last_page:
            sql = sample_table_info_sql(page_size)
            last_page = page_size
        crsr.execute(sql, page)
        for sample_name, id_sample_tmp, uuid_sample_lims in crsr.fetchall():
            for row in idx[str(sample_name)]:
                row["id_sample_tmp"] = id_sample_tmp
                row["uuid_sample_lims"] = uuid_sample_lims


def sample_table_info_sql(page_size: int):
    placeholders = ",".join(["%s"] * page_size)
    return cleandoc(f"""
      SELECT
        name,
        id_sample_tmp,
        uuid_sample_lims
      FROM
        sample
      WHERE
        name IN ({placeholders})
    """)  # noqa: S608


def store_tol_bioproject_rows(
    conn: MySQLConnectionAbstract,
    mlwh_rows: list[dict[str, Any]],
) -> None:
    """
    Syncs all the rows in the `mlwh_rows` argument with the MLWH
    `tol_sample_bioproject` table, or if there are any errors rolls back a
    transaction and exits the script having stored no changes.
    """
    crsr = conn.cursor(dictionary=True)
    sql = tol_sample_bioproject_insert()

    try:
        for row in mlwh_rows:
            # Uses keys in the row dict to fill in the named placeholders in
            # the SQL template:
            crsr.execute(sql, row)
            if not row.get("id_tsb_tmp"):
                # New row _should_ have an auto-incremented ID
                row["id_tsb_tmp"] = crsr.lastrowid or None
    except mysql.connector.Error as err:
        conn.rollback()
        msg = f"Error storing MLWH tol_sample_bioproject row {row!r}: {err}"
        sys.exit(msg)
    conn.commit()


def tol_sample_bioproject_insert() -> str:
    """
    Composes the SQL statement which inserts or updates a row in the
    `tol_sample_bioproject` table.
    """
    columns = (
        "data_id",
        "id_sample_tmp",
        "uuid_sample_lims",
        "file",
        "filename",
        "platform",
        "instrument",
        "library_name",
        "library_source",
        "library_selection",
        "library_strategy",
        "library_type",
        "library_construction_protocol",
        "design_description",
        "tolid",
        "biosample_accession",
        "bioproject_accession",
    )

    cols_str = ",\n          ".join(columns)
    vals_str = ",\n          ".join(f"%({c})s" for c in columns)
    upds_str = ",\n          ".join(f"{c} = %({c})s" for c in columns)

    return cleandoc(f"""
      INSERT INTO
        tol_sample_bioproject (
          {cols_str}
        )
      VALUES
        (
          {vals_str}
        )
      ON DUPLICATE KEY UPDATE
        {upds_str}
    """)  # noqa: S608
