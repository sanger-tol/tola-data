import csv
import datetime
import inspect
import pathlib
import re
import textwrap
from functools import cache

import click
import duckdb

FILE_TYPE = click.Path(
    dir_okay=False,
    exists=True,
    readable=True,
)


@cache
def today_dir():
    today = datetime.date.today().isoformat()  # noqa: DTZ011
    return pathlib.Path(f"pacbio_{today}")


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
    default=("data/pacbio_tolqc_data.csv", "data/pacbio_run_report.csv"),
)
@click.option(
    "--duckdb-file",
    type=click.Path(path_type=pathlib.Path),
    help="Name of duckdb database file.",
    show_default=True,
)
@click.option(
    "--dir",
    "report_dir",
    default=today_dir(),
    type=click.Path(path_type=pathlib.Path),
    help="Directory to store duckdb database and report in",
    show_default=True,
)
@click.pass_context
def cli(ctx, csv_files, report_dir, duckdb_file):
    report_dir.mkdir(exist_ok=True, parents=True)
    if duckdb_file is None:
        duckdb_file = report_dir / "pacbio.duckdb"
    db_exists = duckdb_file.exists()

    create_flag = False
    if db_exists:
        csv_mtime = max(modification_timestamp(p) for p in csv_files)
        ddb_mtime = modification_timestamp(duckdb_file)
        if csv_mtime > ddb_mtime:
            create_flag = True
            duckdb_file.unlink()
            click.echo(
                f"DuckDB database file '{duckdb_file}' is older than"
                " most recent CSV: recreating",
                err=True,
            )
        else:
            click.echo(f"DuckDB database file '{duckdb_file}' already exists", err=True)
    else:
        create_flag = True

    con = duckdb.connect(str(duckdb_file))
    if create_flag:
        click.echo(
            f"Creating and populating DuckDB database '{duckdb_file}'"
            f" from CSV files {csv_files}",
            err=True,
        )
        try:
            create_and_populate_database(con, csv_files)
        except Exception as e:
            duckdb_file.unlink(missing_ok=True)
            msg = "Error creating DuckDB database"
            raise Exception(msg) from e

    table_row_counts(con)
    full_report_grouped_by_idx(con, report_dir / "pacbio_partitioned.csv")
    multi_specimen_csv = report_dir / "pacbio_multi_specimens.csv"
    report_multi_specimen_idx(con, multi_specimen_csv)
    filter_sample_swaps(
        multi_specimen_csv, report_dir / "pacbio_multi_specimen_swaps.csv"
    )
    missing_from_rprt(con, report_dir / "pacbio_missing_from_rprt.csv")


def filter_sample_swaps(multi_specimen_csv, csv_out):
    in_path = pathlib.Path(multi_specimen_csv)
    out_path = pathlib.Path(csv_out)
    out_rows = []
    for chunk in idx_chunks(in_path):
        tol_id_specimens = valid_speciemns_in_chunk(chunk)
        if len(tol_id_specimens) > 1:
            # click.echo(f"Specimens: {tol_id_specimens}", err=True)
            out_rows.extend(chunk)
    if out_rows:
        header = tuple(out_rows[0])
        csv_wrtr = csv.DictWriter(out_path.open("w"), header)
        csv_wrtr.writeheader()
        for row in out_rows:
            csv_wrtr.writerow(row)
    click.echo(f"\nWrote likely specimen swaps into file: '{csv_out}'", err=True)


def idx_chunks(path):
    chunk = []
    current_idx = ""
    for row in csv.DictReader(path.open()):
        if row["idx"] != current_idx:
            if chunk:
                yield chunk
            chunk = [row]
            current_idx = row["idx"]
        else:
            chunk.append(row)
    if chunk:
        # chunk_rows = "".join(f"  {x['idx']}  {x['dup']}  {x['specimen']}\n" for x in chunk)
        # click.echo(f"Last chunk:\n{chunk_rows}", err=True)
        yield chunk
        chunk = None


def valid_speciemns_in_chunk(chunk):
    tol_set = set()
    for row in chunk:
        if m := re.search(
            r"([a-z]{1,2}[A-Z][a-z]{2}[A-Z][a-z]{2,3}\d+)", row["specimen"]
        ):
            tol_set.add(m.group(1))
    return tol_set


def create_and_populate_database(con, csv_files):
    json_csv, rprt_csv = csv_files

    con.begin()

    # Create tables
    tables = "j", "r", "u"
    for name in tables:
        make_report_table(con, name)

    # Populate j and r tables from CSV files
    con.execute("INSERT INTO j SELECT * FROM read_csv_auto(?)", (json_csv,))
    con.execute("INSERT INTO r SELECT * FROM read_csv_auto(?)", (rprt_csv,))

    # Copy data from j and r tables into u
    con.execute("INSERT INTO u SELECT * FROM j")
    con.execute("INSERT INTO u SELECT * FROM r")

    con.commit()


def missing_from_rprt(con, csv_file):
    sql = """
        SELECT j.idx
          , ROW_NUMBER() OVER same_idx AS dup
          , COUNT(*) OVER same_idx AS n_dup
          , j.source
          , j.project
          , j.specimen
          , j.* EXCLUDE(idx, source, project, specimen)
        FROM j
        LEFT JOIN r
          ON j.idx = r.idx
        WHERE r.idx IS NULL
          AND j.model IN ('Sequel IIe', 'Revio')
        WINDOW same_idx AS (
          PARTITION BY j.idx
          ORDER BY j.source, j.project, j.specimen
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )
        ORDER BY j.date, j.idx, dup
    """
    sql_to_csv(con, sql, csv_file)


def full_report_grouped_by_idx(con, csv_file):
    """
    Full report grouped by idx

    idx is the movie name plus "#tag_index" (if any).
    """

    sql = """
        SELECT idx
          , ROW_NUMBER() OVER same_idx AS dup
          , COUNT(*) OVER same_idx AS n_dup
          , source
          , project
          , specimen
          , * EXCLUDE(idx, source, project, specimen)
        FROM u
        WINDOW same_idx AS (
          PARTITION BY idx
          ORDER BY source, project, specimen
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )
        ORDER BY idx, dup
    """
    sql_to_csv(con, sql, csv_file)


def report_multi_specimen_idx(con, csv_file):
    """
    Report where there is more than one specimen per idx
    """

    sql = """
        WITH dups AS (
          SELECT idx
          , ROW_NUMBER() OVER same_idx AS dup
          , COUNT(*) OVER same_idx AS n_dup
          , COUNT(DISTINCT IFNULL(specimen, '')) OVER same_idx AS n_spcmn
          , source
          , project
          , specimen
          , * EXCLUDE(idx, source, project, specimen)
          FROM u
          WINDOW same_idx AS (
            PARTITION BY idx
            ORDER BY source, project, specimen
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          )
        )
        SELECT * EXCLUDE(n_spcmn) FROM dups
        WHERE n_spcmn > 1
        ORDER BY idx, dup
    """
    sql_to_csv(con, sql, csv_file)


def sql_to_csv(con, raw_sql, csv_file):
    sql = inspect.cleandoc(raw_sql)
    con.sql(sql).write_csv(str(csv_file), header=True)
    info_sql = textwrap.indent(sql, "  ")
    click.echo(f"\nWrote query:\n{info_sql}\nto file: '{csv_file}'")


def table_row_counts(con):
    for row in con.sql("SHOW TABLES").fetchall():
        (tbl,) = row
        (count,) = con.sql(f"SELECT COUNT(*) FROM '{tbl}'").fetchone()
        click.echo(f"Table '{tbl}' has {count:6d} rows", err=True)


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


def modification_timestamp(pth):
    if isinstance(pth, str):
        pth = pathlib.Path(pth)
    return pth.lstat().st_mtime


if __name__ == "__main__":
    cli()
