import click
import csv
import datetime
import duckdb
import io
import os
import pathlib
import re
import requests
import sys
import tempfile


def status_db_today():
    today = datetime.date.today().isoformat()
    return f"tol_status_{today}.duckdb"


@click.command(
    help=(
        "Download ToL QC Status spreadsheets and"
        " construct a duckdb database from them"
    )
)
@click.option(
    "--duckdb-file",
    default=status_db_today(),
    type=click.Path(path_type=pathlib.Path),
    help="Name of duckdb database file.",
    show_default=True,
)
def cli(duckdb_file):
    con = duckdb.connect(str(duckdb_file))
    con.begin()
    click.echo(f"Created duckdb database is '{duckdb_file}'", err=True)

    # ID of "Tree of Life assembly informatics" Google Sheet
    document_id = "1RKubj10g13INd4W7alHkwcSVX_0CRvNq0-SRe21m-GM"

    db_sheets = {
        "Status": "status",
        "ASG": "asg",
        "Cobiont Submission": "cobiont_sub",
        "Primary Metagenome Submission": "primary_meta_sub",
        "Binned Metagenome Submission": "binned_meta_sub",
        "Raw data submission": "raw_data_sub",
        "BioProjects": "bio_projects",
    }

    for sheet_name, sheet_table in db_sheets.items():
        try:
            data = fetch_sheet_io(document_id, sheet_name)
            create_table(con, sheet_table, data)
        except Exception as e:
            con.rollback()
            msg = f"Error creating table '{sheet_table}' from sheet '{sheet_name}'"
            raise Exception(msg) from e

    con.execute("ALTER TABLE status RENAME COLUMN sample TO specimen")

    con.commit()

    # Start duckdb cli if run in a terminal
    if sys.stdout.isatty():
        con.close()
        os.execlp("duckdb", "duckdb", str(duckdb_file))


def create_table(con, table_name, data):
    csv_in = csv.reader(data)
    try:
        header = next(csv_in)
    except StopIteration:
        msg = "No header. Empty file?"
        raise ValueError(msg)

    # Exclue any leading and trailing columns with an empty header
    first_col, last_col = None, None
    for i, v in enumerate(header):
        if v:
            if not first_col:
                first_col = i
            last_col = i + 1
    if first_col is None:
        msg = "All header columns are empty. Blank first line in sheet?"
        raise ValueError(msg)

    header = cleanup_header(header[first_col:last_col])

    # Create temporary CSV file and write header and body
    csv_tmp = tempfile.NamedTemporaryFile(
        "w", prefix="sheets_to_duckdb_", suffix=".csv"
    )
    csv_out = csv.writer(csv_tmp)
    csv_out.writerow(header)
    for line in csv_in:
        csv_out.writerow(line[first_col:last_col])

    # Ensure data is flushed to storage
    csv_tmp.flush()
    os.fsync(csv_tmp.fileno())

    # Import the data from the temporary CSV file into duckdb
    stmt = f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv_auto(?)"
    con.execute(stmt, (csv_tmp.name,))


def fetch_sheet_io(document_id, sheet_name):
    url = f"https://docs.google.com/spreadsheets/d/{document_id}/gviz/tq"
    r = requests.get(url, params={"sheet": sheet_name, "tqx": "out:csv"})
    if r.status_code == requests.codes.ok:
        return io.StringIO(r.text)
    else:
        r.raise_for_status()


def cleanup_header(dirty):
    return tuple(make_identifier(x) for x in dirty)


def make_identifier(txt):
    idtfyr = re.sub(r"\W+", "_", re.sub(r"&+", "_and_", txt))
    return idtfyr.strip("_")


if __name__ == "__main__":
    cli()
