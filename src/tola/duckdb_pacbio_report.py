import click
import duckdb
import inspect
import pathlib
import sys

from functools import cache

FILE_TYPE = click.Path(
    dir_okay=False,
    exists=True,
    readable=True,
    path_type=pathlib.Path,
)


@click.command(
    help=(
        "Generate reports comparing information from PacBio data.json and"
        " run report, using data from CSV files created by compare_pacbio_json"
        " from TolQC and run report respectively."
    )
)
@click.argument(
    "csv_files",
    nargs=2,
    type=(FILE_TYPE, FILE_TYPE),
    default=("pacbio_tolqc_data.csv", "pacbio_run_report.csv"),
)
@click.option(
    "--duckdb-file",
    default="pacbio.duckdb",
    type=click.Path(path_type=pathlib.Path),
    help="Name of duckdb database file.",
    show_default=True,
)
@click.pass_context
def cli(ctx, csv_files, duckdb_file):
    if duckdb_file.exists():
        ctx.fail(f"DuckDB database file '{duckdb_file}' already exists")
    con = duckdb.connect(str(duckdb_file))
    tables = "j", "r", "u"
    for name in tables:
        make_report_table(con, name)


def make_report_table(con, name):
    sql = report_table_sql().format(name)
    con.execute(sql)


@cache
def report_table_sql():
    return inspect.cleandoc(
        """
        CREATE TABLE {} (idx VARCHAR
          , source VARCHAR
          , movie_name VARCHAR
          , tag_index BIGINT
          , project VARCHAR
          , specimen VARCHAR
          , sample_id VARCHAR
          , pipeline VARCHAR
          , platform VARCHAR
          , model VARCHAR
          , date DATE
          , lims_qc VARCHAR
          , run VARCHAR
          , well VARCHAR
          , instrument VARCHAR
          , movie_length INT
          , tag VARCHAR
          , sample_accession VARCHAR
          , run_accession VARCHAR
          , library_load_name VARCHAR
          , reads INT
          , bases BIGINT
          , mean DOUBLE
          , n50 INT
          , species VARCHAR
          , loading_conc DOUBLE
          , binding_kit VARCHAR
          , sequencing_kit VARCHAR
          , include_kinetics BOOLEAN
        )
        """
    )


if __name__ == "__main__":
    cli()
