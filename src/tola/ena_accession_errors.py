import re
import sys
import textwrap
from io import StringIO

import click

from tola import click_options
from tola.pretty import wrap_name_description
from tola.store_ena_assemblies import (
    cache_ena_assemblies,
    cache_tolqc_assemblies,
    cache_tolqc_assembly_datasets,
)
from tola.tolqc_client import TolClient

REPORTS = {
    "RNA": {
        "description": "RNA-Seq run accessions listed in ENA assemblies",
        "query": """
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
            rna.specimen,
            genome_accession_id,
            run_accession,
            data_id,
            dataset_id,
            dataset_name,
          FROM
            rna
          JOIN tolqc USING (genome_accession_id)
        """,
    },
    "no-dataset": {
        "description": (
            "Run accessions listed in ENA assemblies which aren't in ToLQC datasets"
        ),
        "query": """
          WITH
            ena_run AS (
              SELECT
                genome_accession_id,
                unnest(run_accession_list) AS run_accession
              FROM
                ena
            )
          SELECT
            specimen,
            genome_accession_id,
            data_type,
            data_id,
            run_accession
          FROM
            asm_data
            JOIN ena_run USING (run_accession)
          WHERE
            dataset_id IS NULL
        """,
    },
    "missing": {
        "description": "Run accessions missing from ENA assemblies",
        "query": """
          WITH
            asm_run AS (
              SELECT
                specimen,
                run_accession,
                data_id,
                data_type
              FROM
                asm_data
              WHERE
                data_type != 'rnaseq'
                AND run_accession IS NOT NULL
            ),
            ena_run AS (
              SELECT
                genome_accession_id,
                unnest(run_accession_list) AS run_accession
              FROM
                ena
            )
          SELECT
            specimen,
            genome_accession_id,
            data_type,
            data_id,
            run_accession
          FROM
            asm_run
            JOIN tolqc USING (specimen)
            ANTI JOIN ena_run USING (run_accession, genome_accession_id)
          WHERE
            tolqc.genome_accession_id IS NOT NULL
        """,
    },
    "bad-specimen-biosample": {
        "description": (
            "ENA assemblies not loaded into ToLQC due to mismatches"
            " in specimen biosample accessions"
        ),
        "query": """
          WITH
            missing AS (
              SELECT
                regexp_extract(assembly_name, '^([^.]+)') AS specimen,
                *
              FROM
                ena
                ANTI JOIN tolqc USING (genome_accession_id)
              WHERE
                -- Use underscores in assembly names to exclude metagenome assemblies
                assembly_name !~ '.*_.*'
            )
          SELECT DISTINCT
            specimen,
            tolqc.specimen_biosample AS tolqc_specimen_biosample,
            missing.specimen_biosample AS ena_specimen_biosample,
            missing.* EXCLUDE (specimen, specimen_biosample)
          FROM
            missing
            JOIN tolqc USING (specimen)
          WHERE
            tolqc_specimen_biosample != ena_specimen_biosample
        """,
    },
    "cross-specimen": {
        "description": (
            "ENA assembly run accessions from other specimens,"
            " excluding Hi-C data with the same ToLID prefix"
        ),
        "query": """
          WITH
            rs AS (
              SELECT
                data_id,
                data_type,
                run_accession,
                specimen,
                unnest(assemblies, recursive := true)
              FROM
                asm_data
            )
          SELECT
            COALESCE(tolqc.cobiont_of, tolqc.specimen) AS tolqc_specimen,
            rs.specimen AS ena_run_specimen,
            rs.genome_accession_id,
            rs.data_type,
            data_id,
            run_accession,
          FROM
            tolqc
            JOIN rs USING (assembly_id)
          WHERE
            tolqc_specimen != ena_run_specimen
            AND NOT (
              data_type = 'hic'
              AND extract_tolid(tolqc_specimen) = extract_tolid(ena_run_specimen)
            )
        """,
    },
    "cross-tolid": {
        "description": "ENA assembly run accessions from other ToLID prefixes",
        "query": r"""
          WITH
            rs AS (
              SELECT
                data_id,
                run_accession,
                extract_tolid(specimen) AS ena_tolid,
                unnest(assemblies, recursive := true)
              FROM
                asm_data
            )
          SELECT DISTINCT
            COALESCE(tolqc.cobiont_of, tolqc.specimen)
              .extract_tolid() AS tolqc_tolid,
            rs.ena_tolid,
            rs.genome_accession_id,
            data_id,
            run_accession,
          FROM
            tolqc
            JOIN rs USING (assembly_id)
          WHERE
            ena_tolid != tolqc_tolid
        """,
    },
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
@click.option(
    "--summary",
    flag_value=True,
    default=False,
    help="Print the number of rows found by each report",
)
@click.pass_context
def cli(ctx, tolqc_alias, report_name, output_file, output_format, summary):
    """
    Report potential errors in linking of ENA run accessions to assemblies.
    """

    if not report_name and not summary:
        name_desc = {k: v["description"] for k, v in REPORTS.items()}
        click.echo(
            ctx.get_help()
            + "\n\nAvailable reports:\n"
            + wrap_name_description(name_desc)
        )
        sys.exit(1)

    out_file, out_sql = duckdb_copy_file_statement(output_file, output_format)

    client = TolClient(
        tolqc_alias=tolqc_alias,
        page_size=1000,
    )

    # Fetch data from ToLQC and the ENA
    conn = client.duckdb_connect()
    add_macros(conn)
    cache_tolqc_assemblies(client, conn)
    cache_tolqc_assembly_datasets(client, conn)
    cache_ena_assemblies(conn)

    if summary:
        rprt = []
        for name, conf in REPORTS.items():
            query = conf["query"]
            description = conf["description"]
            (n,) = conn.execute(f"SELECT COUNT(*) FROM ({query})").fetchone()  # noqa: S608  # ty:ignore[not-iterable]
            rprt.append((f"{n:,d}", name, description))
        click.echo("\nReport summary:\n" + wrap_report(rprt))

    else:
        query = REPORTS[report_name]["query"]
        sql = out_sql.format(query)
        conn.execute(sql, [out_file])


def add_macros(conn):
    conn.execute(r"""
    CREATE OR REPLACE MACRO extract_tolid (s) AS
      s.regexp_extract('^([^.]+)')
       .regexp_replace('\d+$', '');
  """)


def duckdb_copy_file_statement(file: str, fmt: str):
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
        opt = "(FORMAT CSV)"
    elif fmt == "TSV":
        opt = "(FORMAT CSV, SEP '\t')"
    elif fmt == "NDJSON":
        opt = "(FORMAT JSON, ARRAY false)"

    sql = "COPY ({} ORDER BY ALL) TO $1 " + opt

    return file, sql


def wrap_report(rprt: list[tuple]):
    max_n = max(len(row[0]) for row in rprt)
    max_name = max(len(row[1]) for row in rprt)

    desc_width = 67 - max_name

    out = StringIO("")
    for n, name, desc in rprt:
        desc = desc.rstrip(".") + "."
        first, *rest = textwrap.wrap(desc, width=desc_width)
        out.write(f"\n {n:>{max_n}}  {name:{max_name}}  {first}\n")
        for txt in rest:
            out.write(f" {' ':>{max_n}}  {' ':{max_name}}  {txt}\n")

    return out.getvalue()
