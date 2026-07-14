import logging
import pathlib
import re
import sys
import textwrap
from io import StringIO

import click

from tola import click_options
from tola.ena.database import EnaCache
from tola.ndjson import get_input_objects
from tola.pretty import bold, wrap_name_description
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
            run_accession,
            data_id,
            data_type,
            reason,
          FROM
            asm_data
            JOIN ena_run USING (run_accession)
            LEFT JOIN error_reason USING (genome_accession_id, run_accession)
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
            run_accession,
            data_id,
            data_type,
            reason,
          FROM
            asm_run
            JOIN tolqc USING (specimen)
            ANTI JOIN ena_run USING (run_accession, genome_accession_id)
            LEFT JOIN error_reason USING (genome_accession_id, run_accession)
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
                NOT assembly_name.contains('_')
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
            rs.run_accession,
            data_id,
            rs.data_type,
            reason,
          FROM
            tolqc
            JOIN rs USING (assembly_id)
            LEFT JOIN error_reason AS ar
              ON rs.genome_accession_id = ar.genome_accession_id
              AND rs.run_accession = ar.run_accession
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
                data_type,
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
            rs.run_accession,
            data_id,
            rs.data_type,
            reason,
          FROM
            tolqc
            JOIN rs USING (assembly_id)
            LEFT JOIN error_reason AS ar
              ON rs.genome_accession_id = ar.genome_accession_id
              AND rs.run_accession = ar.run_accession
          WHERE
            ena_tolid != tolqc_tolid
        """,
    },
}


@click.command
@click_options.tolqc_alias
@click.option(
    "--duckdb-file",
    "duckdb_file",
    default=None,
    help="""
      Path to the DuckDB database file which caches ENA assembly accession
      data. If not specifed it defaults to the value of the
      ENA_ASSEMBLY_ACCESSIONS_DUCKDB environment variable if set, or else
      uses a temporary in-memory database.
    """,
    envvar="ENA_ASSEMBLY_ACCESSIONS_DUCKDB",
    type=click.Path(
        path_type=pathlib.Path,
    ),
    show_default=True,
)
@click.option(
    "--update",
    "update_flag",
    flag_value=True,
    default=False,
    show_default=True,
    help="""
      Update the DuckDB cache database from ToLQC and the ENA.  The cache will
      also be updated without this flag if it is more than four hours old.
    """,
)
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
@click.option(
    "--show-reason-dict",
    flag_value=True,
    help="""
      Show the dictionary of reasons for run accessions to be missing from ENA
      assemblies.
    """,
)
@click.option(
    "--add-reason-dict",
    nargs=2,
    metavar=("REASON", "DESCRIPTION"),
    help="Add a reason and its description to the dictionary of reasons.",
)
@click.option(
    "--store-reason",
    metavar="REASON",
    help="""
      Name of reason to store for each genome accession ID / run accession ID
      pair input.  Each NDJSON row from the INPUT_FILES argument is
      expected to have a value under `genome_accession_id` for the genome
      accession, and another under `run_accession_id` for the run accession.
    """,
)
@click.option(
    "--delete-reason",
    metavar="REASON",
    help="""
      Name of reason to delete for each genome accession ID / run accession ID
      pair input.  Input is the same as for `--store-reason`.
    """,
)
@click_options.input_files
@click_options.log_level
@click.pass_context
def cli(
    ctx,
    tolqc_alias,
    duckdb_file,
    update_flag,
    report_name,
    output_file,
    output_format,
    summary,
    show_reason_dict,
    add_reason_dict,
    store_reason,
    delete_reason,
    input_files,
    log_level,
):
    """
    Report potential errors in linking of ENA run accessions to assemblies.
    """

    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(message)s",
        force=True,
    )

    # Fetch data from ToLQC and the ENA
    client = TolClient(
        tolqc_alias=tolqc_alias,
        page_size=1000,
    )
    cache = EnaCache(duckdb_file, client)

    out_file, out_sql = duckdb_copy_file_statement(output_file, output_format)

    if update_flag or cache.needs_update:
        cache.cache_tolqc_assemblies()
        cache.cache_tolqc_assembly_datasets()
        cache.cache_ena_assemblies()
        cache.log_update_time()

    if summary or update_flag:
        rprt = []
        for name, conf in REPORTS.items():
            query = conf["query"]
            description = conf["description"]
            count = cache.execute(f"SELECT COUNT(*) FROM ({query})").fetchone()  # noqa: S608
            n = 0 if count is None else count[0]
            rprt.append((f"{n:,d}", name, description))
        click.echo("\nNumber of rows in each report:\n" + wrap_report(rprt))
    elif report_name:
        query = REPORTS[report_name]["query"]
        sql = out_sql.format(query)
        cache.execute(sql, [out_file])
    elif show_reason_dict:
        cache.show_reason_dict_contents()
    elif add_reason_dict:
        cache.load_reason_dict_entry(add_reason_dict)
    elif store_reason:
        input_objects = get_input_objects(input_files)
        n = cache.store_error_reasons(store_reason, input_objects)
        click.echo(f"Stored {bold(n)} {store_reason!r} error reasons", err=True)
    elif delete_reason:
        input_objects = get_input_objects(input_files)
        n = cache.delete_error_reasons(delete_reason, input_objects)
        click.echo(f"Deleted {bold(n)} {delete_reason!r} error reasons", err=True)
    else:
        name_desc = {k: v["description"] for k, v in REPORTS.items()}
        sys.exit(
            ctx.get_help()
            + "\n\nAvailable reports:\n"
            + wrap_name_description(name_desc)
        )


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
