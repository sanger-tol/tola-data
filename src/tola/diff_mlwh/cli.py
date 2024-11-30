import logging
import pathlib
import sys
from tempfile import NamedTemporaryFile

import click

from tola import click_options, tolqc_client
from tola.diff_mlwh.column_definitions import table_map
from tola.diff_mlwh.database import run_mlwh_diff
from tola.diff_mlwh.diff_store import write_pretty_output
from tola.fetch_mlwh_seq_data import fetch_mlwh_seq_data_to_file
from tola.ndjson import ndjson_row


@click.command
@click_options.tolqc_alias
@click_options.tolqc_url
@click_options.api_token
@click_options.log_level
@click_options.diff_mlwh_duckdb
@click.option(
    "--update/--no-update",
    help="Update ToLQC and MLWH data from their source databases",
    default=False,
    show_default=True,
)
@click.option(
    "--mlwh-ndjson",
    type=click.Path(
        path_type=pathlib.Path,
    ),
    envvar="MLWH_NDJSON",
    help="""Name of NDJSON file from fetch-mlwh-seq-data.
      Taken from the MLWH_NDJSON environment variable if set""",
    hidden=True,
)
@click.option(
    "--new/--all",
    "show_new_diffs",
    help="""Print the most recently detected mismatches to STDOUT
      instead of the default of printing all stored mismatches.
      Overridden by the --since or --today options""",
    default=False,
    show_default=True,
)
@click.option(
    "--today",
    "since",
    flag_value=click_options.TODAY,
    type=click.DateTime(),
    help="Only show differences found today",
)
@click.option(
    "--since",
    "since",
    type=click.DateTime(),
    help="Show differences detected since a particular date or time",
)
@click.option(
    "--show-classes",
    flag_value=True,
    help="""Show the list of differences grouped by the names of columns
      which differ and their counts""",
)
@click.option(
    "--class",
    "column_class",
    help="""Show the differences with this combination of columns which differ.
      (See output from --show-classes)""",
)
@click.option(
    "--show-reason-dict",
    flag_value=True,
    help="""Show the dictionary of reasons for differences between the MLWH and
      ToLQC databases.""",
)
@click.option(
    "--add-reason-dict",
    nargs=2,
    metavar=("REASON", "DESCRIPTION"),
    help="""Add a reason and its description to the dictionary of reasons""",
)
@click.option(
    "--reason",
    metavar="REASON",
    help="""Show the differences tagged with this reason.
      The value "NONE" will show any differences not tagged with a reason.
      (See output from --show-reason-dict for the list of reasons)""",
)
@click.option(
    "--store-reason",
    metavar="REASON",
    help="Store the reason for the list of `data_id` supplied",
)
@click.option(
    "--delete-reason",
    metavar="REASON",
    help="Delete the reason for the list of `data_id` supplied",
)
@click.argument(
    "data_id_list",
    metavar="DATA_ID_LIST",
    nargs=-1,
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(
        ["PRETTY", "NDJSON"],
        case_sensitive=False,
    ),
    help="""Output differences found in either 'PRETTY'
      (human readable) or 'NDJSON' format.
      Defaults to 'PRETTY' if stdout is a terminal, else 'NDJSON'""",
)
@click.option(
    "--show",
    "show_columns",
    multiple=True,
    help="""Columns to show. Can be specified multiple time or as a comma
      separated list. The value "ALL" will show all columns, except those
      which are null in both MLWH and ToLQC.""",
)
@click.option(
    "--table",
    help="Name of table for which to print patching NDJSON",
)
def cli(
    tolqc_alias,
    tolqc_url,
    api_token,
    log_level,
    diff_mlwh_duckdb,
    mlwh_ndjson,
    show_new_diffs,
    show_classes,
    column_class,
    show_reason_dict,
    add_reason_dict,
    reason,
    store_reason,
    delete_reason,
    data_id_list,
    output_format,
    show_columns,
    since,
    table,
    update,
):
    """
    Compare the contents of the MLWH to the ToLQC database

    DATA_ID_LIST is a list of `data_id` values to act on.
    """

    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(message)s",
        force=True,
    )
    tqc = tolqc_client.TolClient(tolqc_url, api_token, tolqc_alias)

    if column_class:
        column_class = column_class.split(",")

    if show_columns:
        sc = []
        for spec in show_columns:
            if spec.upper() == "ALL":
                sc = ["ALL"]
                break
            sc.extend(spec.split(","))
        show_columns = set(sc)
    else:
        show_columns = None

    reason_action = None
    if store_reason:
        reason_action = "STORE"
        reason = store_reason
    elif delete_reason:
        reason_action = "DELETE"
        reason = delete_reason

    if not output_format:
        output_format = "PRETTY" if sys.stdout.isatty() else "NDJSON"

    mlwh_tmp = None
    if update:
        if mlwh_ndjson and mlwh_ndjson.exists():
            logging.info(f"Loading MLWH data from {mlwh_ndjson}")
        else:
            mlwh_tmp = NamedTemporaryFile("r", prefix="mlwh_", suffix=".ndjson")
            logging.info(f"Downloading data from MLWH into {mlwh_tmp.name}")
            mlwh_ndjson = pathlib.Path(mlwh_tmp.name)
            fetch_mlwh_seq_data_to_file(tqc, mlwh_ndjson)
    else:
        mlwh_ndjson = None

    diffs = run_mlwh_diff(
        tqc,
        diff_mlwh_duckdb=diff_mlwh_duckdb,
        mlwh_ndjson=mlwh_ndjson,
        show_new_diffs=show_new_diffs,
        show_classes=show_classes,
        column_class=column_class,
        show_reason_dict=show_reason_dict,
        add_reason_dict=add_reason_dict,
        reason=reason,
        reason_action=reason_action,
        data_id_list=data_id_list,
        since=since,
    )

    # Cleanup temporary file
    mlwh_tmp = None

    if not diffs:
        exit(0)

    if table:
        write_table_patch(diffs, table, sys.stdout)
    else:
        if output_format == "PRETTY":
            write_pretty_output(diffs, show_columns, sys.stdout)
        else:
            for m in diffs:
                sys.stdout.write(ndjson_row(m.differences_dict(show_columns)))


def write_table_patch(diffs, table, filehandle):
    col_map = table_map().get(table)
    if not col_map:
        exit(f"No column map for table '{table}'")
    for m in diffs:
        if patch := m.get_patch_for_table(table, col_map):
            filehandle.write(ndjson_row(patch))
