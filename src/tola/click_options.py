import datetime
import pathlib

import click

TODAY = datetime.date.today().isoformat()  # noqa: DTZ011

tolqc_alias = click.option(
    "--tolqc-alias",
    help="Name of connection parameters alias in ~/.connection_params.json file.",
    default="tolqc",
    show_default=True,
)

tolqc_url = click.option(
    "--tolqc-url",
    envvar="TOLQC_URL",
    help=(
        "URL of ToL QC API. Overrides TOLQC_URL environment variable if set,"
        " which in turn overrides the ~/.connection_params.json value of"
        " `api_url` for the alias."
    ),
)

api_token = click.option(
    "--api-token",
    envvar="TOLQC_API_KEY",
    help=(
        "Token for the ToL QC API. Overrides TOLQC_API_KEY environment variable"
        " if set, which in turn overrides the ~/.connection_params.json value"
        " of `api_token` for the alias."
    ),
)

file_format = click.option(
    "--format",
    "file_format",
    type=click.Choice(
        ["NDJSON", "TXT"],
        case_sensitive=False,
    ),
    default=None,
    show_default=True,
    help="Format of input file(s) or STDIN",
)

log_level = click.option(
    "--log-level",
    type=click.Choice(
        ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        case_sensitive=False,
    ),
    default="WARNING",
    hidden=True,
    help="Diagnostic messages to show.",
)

input_files = click.argument(
    "input_files",
    nargs=-1,
    required=False,
    type=click.Path(
        path_type=pathlib.Path,
        exists=True,
        readable=True,
    ),
)


def default_diff_mlwh_duckdb():
    return pathlib.Path(f"diff_mlwh_{TODAY}.duckdb")


diff_mlwh_duckdb = click.option(
    "--duckdb-file",
    "diff_mlwh_duckdb",
    type=click.Path(path_type=pathlib.Path),
    help="""Name of duckdb database file which caches MLWH mismatches.
      Taken from the DIFF_MLWH_DUCKDB environment variable if set""",
    default=default_diff_mlwh_duckdb(),
    envvar="DIFF_MLWH_DUCKDB",
    show_default=True,
)

table = click.option(
    "--table",
    required=True,
    help="Name of table in ToLQC database",
)

key = click.option(
    "--key",
    default="id",
    show_default=True,
    help=(
        "Column name use to uniquely identify rows."
        " Defaults to the table's `.id` column"
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
    help="Input file names.",
)

id_list = click.argument(
    "id_list",
    nargs=-1,
    required=False,
)

apply_flag = click.option(
    "--apply/--dry",
    "apply_flag",
    default=False,
    show_default=True,
    help="Apply changes or perform a dry run and show changes which would be made.",
)

write_to_stdout = click.option(
    "--stdout/--server",
    "write_to_stdout",
    default=False,
    show_default=True,
    help="""
    Writes the fetched data to STDOUT as NDJSON instead of saving to the ToLQC
    database.
    """,
)
