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
