import re
import sys

import click
import duckdb

from tola import click_options
from tola.ndjson import get_input_objects, ndjson_row
from tola.pretty import plain_text_from_itr, wrap_name_description
from tola.store_ena_assemblies import (
    cache_ena_assemblies,
    cache_tolqc_assemblies,
    cache_tolqc_assembly_datasets,
)
from tola.terminal import TerminalDict, colour_pager, s
from tola.tolqc_client import TolClient

REPORTS = {
    "RNA": "RNA-Seq run accessions listed in assemblies",
}


@click.command
@click_options.tolqc_alias
@click.option(
    "--report",
    "report_name",
    type=click.Choice(
        list(REPORTS),
        case_sensitive=False,
    ),
    help="Name of report to produce",
)
@click.option(
    "--output-file",
    "-o",
    help="""
      Name of output file.  Defaults to STDOUT.  Format is guessed from the
      file's extension when given.
    """,
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(
        ["CSV", "TSV", "NDJSON"],
        case_sensitive=False,
    ),
    help="Output format of the report",
)
@click.pass_context
def cli(ctx, tolqc_alias, report_name, output_file, output_format):
    """
    Report potential errors in linking of ENA run accessions to assemblies.
    """

    if not report_name:
        click.echo(
            ctx.get_help() + "\n\nAvailable reports:\n" + wrap_name_description(REPORTS)
        )
        sys.exit(1)

    out_file, out_sql = format_duckdb_output(output_file, output_format)

    client = TolClient(
        tolqc_alias=tolqc_alias,
        page_size=1000,
    )

    # Fetch data from ToLQC and the ENA
    conn = client.duckdb_connect()
    cache_tolqc_assemblies(client, conn)
    cache_tolqc_assembly_datasets(client, conn)
    cache_ena_assemblies(conn)

    if report_name == "RNA":
        print_rna_report(conn, out_file, out_sql)


def format_duckdb_output(file: str, fmt: str):
    if not fmt:
        if file:
            if m := re.search(r"\.(CSV|TSV|NDJSON)$", file, re.IGNORECASE):
                fmt = m.group(1).upper()
            else:
                sys.exit(f"Cannot determine file fmt from filename: {file!r}")
        else:
            fmt = "NDJSON"
    if not file:
        file = "/dev/stdout"

    if fmt == "CSV":
        sql = "COPY ({}) TO $1 (FORMAT CSV)"
    elif fmt == "TSV":
        sql = "COPY ({}) TO $1 (FORMAT CSV, SEP '\t')"
    elif fmt == "NDJSON":
        sql = "COPY ({}) TO $1 (FORMAT JSON, ARRAY false)"

    return file, sql


def print_rna_report(conn: duckdb.DuckDBPyConnection, out_file: str, out_sql: str):
    query = """
      WITH
        rna AS (
          SELECT
            * EXCLUDE (assemblies),
            unnest(assemblies, recursive := true)
          FROM
            asm_data
          WHERE
            data_type = 'rnaseq'
            AND assemblies IS NOT NULL
        )
      SELECT
        specimen,
        genome_accession_id,
        run_accession,
        dataset_id,
        dataset_name,
        status,
        status_time
      FROM
        rna
      JOIN tolqc USING (genome_accession_id)
      ORDER BY ALL
    """
    sql = out_sql.format(query)
    conn.execute(sql, [out_file])
