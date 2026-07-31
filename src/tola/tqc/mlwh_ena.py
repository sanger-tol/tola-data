import logging
import sys
from collections.abc import Callable
from inspect import cleandoc
from io import StringIO
from string import Formatter
from typing import Any

import click
import mysql.connector
from mysql.connector.abstracts import MySQLConnectionAbstract

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
    conn = db_connection.mlwh_rw_db()

    if store_flag:
        mlwh_rows = input_objects_or_exit(ctx, file_list)
    else:
        data_id_list = list(
            id_iterator("data.id", data_id_list, file_list, file_format)
        )
        filt_spec = build_filter_spec(data_id_list)
        tolqc_rows = fetch_tolqc_rows(client, filt_spec)
        fetch_sample_table_fields(conn, tolqc_rows, client)
        add_extra_mlwh_info(conn, tolqc_rows)
        mlwh_rows = build_tol_bioproject_rows(tolqc_rows)

    if not mlwh_rows:
        sys.exit(0)

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


def fetch_tolqc_rows(
    client: TolClient, filt_spec: dict[str, dict[str, Any]]
) -> list[dict[str, str]]:
    """
    Returns a list of `file` table entries satisfying the filter
    specification.  Each row is a flattened dict of the required
    information.
    """

    return list(
        client.ads_get_dict_list(
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
                "data.run.id",
            ],
        )
    )


def add_extra_mlwh_info(
    conn: MySQLConnectionAbstract, tolqc_rows: list[dict[str, Any]]
) -> None:
    pass


def build_tol_bioproject_rows(tolqc_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Checks and reformats the info for each `data.id` in `tolqc_rows` for the
    MLWH `tol_sample_bioproject` table.  Filters out any rows missing
    information and logs a warning showing what's missing.
    """

    mlwh_rows = []
    for file in tolqc_rows:
        if row := _build_tol_bioproject_row(file):
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


def format_description(flat: dict[str, Any], missing: list[str]) -> str | None:
    """
    Formats a description or appends to the `missing` array if data required
    to fill out a template is missing.
    """

    # Is there a description template?
    for tmpl_source in (
        "data.library.description",
        "data.library.library_type.description_template",
    ):
        if template := flat[tmpl_source]:
            break
    if not template:
        return None

    # Fill out the template. (This will return the string unaltered if there
    # are no "{name}" format directives in the string.)
    desc = StringIO("")
    for txt, source, *_ in Formatter().parse(template):
        if txt is not None:
            desc.write(txt)

        if source is None:
            # Text at the end of the template string
            continue
        elif source == "":
            # Invalid "{}" format directive in string - a name is required
            missing.append(f"Missing field name in {tmpl_source} template: {template}")
            return

        val = flat.get(source)
        if val is None:
            missing.append(source)
            return
        desc.write(str(val))

    return desc.getvalue()


# ruff: disable[E501]
WANTED_MAP = (
    # Name                             Source                                             Required
    ("data_id",                        "data.id",                                         True),
    ("sample_name",                    "data.sample.id",                                  True),
    ("id_sample_tmp",                  "id_sample_tmp",                                   True),
    ("uuid_sample_lims",               "uuid_sample_lims",                                True),
    ("file",                           "remote_path",                                     True),
    ("filename",                       "name",                                            True),
    ("platform",                       "data.run.platform.name",                          True),
    ("instrument",                     "data.run.platform.model",                         True),
    ("library_name",                   "data.library.id",                                 True),
    ("library_source",                 "data.library.library_type.source",                True),
    ("library_selection",              "data.library.library_type.selection",             True),
    ("library_strategy",               "data.library.library_type.strategy",              True),
    ("library_type",                   "data.library.library_type.id",                    True),
    ("library_construction_protocol",  "data.library.library_type.id",                    True),
    ("design_description",             format_description,                                False),
    ("tolid",                          "data.sample.specimen.id",                         True),
    ("biosample_accession",            "data.sample.accession.id",                        True),
    ("bioproject_accession",           "data.sample.specimen.species.data_accession.id",  True),
    ("run",                            "data.run.id",                                     True),
    ("cut_sites",                      "data.library.library_type.cut_sites",             False),
)  # fmt: skip
# ruff: enable[E501]


def _build_tol_bioproject_row(flat: dict[str, Any]):

    # Check that all the required values are present
    missing = []
    row = {}
    for name, source, required in WANTED_MAP:
        if isinstance(source, Callable):
            val = source(flat, missing)
        else:
            val = flat.get(source)
            if val is None:
                if required:
                    missing.append(source)
            elif name == "file":
                # Cannot proceed without an iRODS file name, since iRODS is where
                # DataHose read files from.
                if not val.startswith("irods:"):
                    missing.append(source)
                else:
                    val = val[6:]

        row[name] = val

    if missing:
        msg = "  \n  ".join(
            [f"Missing value{s(missing)} for data.id = {row['data_id']!r}:", *missing]
        )
        log.warning(msg)
        return

    return row


def fetch_sample_table_fields(
    conn: MySQLConnectionAbstract,
    mlwh_rows: list[dict[str, Any]],
    client: TolClient,
) -> None:

    idx: dict[str, list[dict[str, Any]]] = {}
    for row in mlwh_rows:
        idx.setdefault(row["data.sample.id"], []).append(row)

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
