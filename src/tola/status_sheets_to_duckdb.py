import datetime
import os
import re
import sys
import tempfile

import click
import duckdb
import requests


def status_db_today():
    today = datetime.date.today().isoformat()  # noqa: DTZ011
    return f"tol_status_{today}.duckdb"


@click.command(
    help=(
        "Download ToL QC Status spreadsheets and"
        " construct a duckdb database from them"
    ),
)
@click.option(
    "--duckdb-file",
    default=status_db_today(),
    help="Name of duckdb database file.",
    show_default=True,
)
def cli(duckdb_file):
    conn = duckdb.connect(duckdb_file)
    conn.begin()
    click.echo(f"Created duckdb database is '{duckdb_file}'", err=True)

    # ID of "Tree of Life assembly informatics" Google Sheet
    document_id = "1RKubj10g13INd4W7alHkwcSVX_0CRvNq0-SRe21m-GM"

    # GID for a sheet can be found in the URL when its tab is selected
    db_sheets_gid = {
        "status": "1442224132",
        "asg": "1822055132",
        "cobiont_sub": "791221292",
        "primary_meta_sub": "1641921323",
        "binned_meta_sub": "688995138",
        "raw_data_sub": "249200423",
        "bio_projects": "728482940",
        "metagenome_tmp": "1641921323",
        "metagenome_bin": "688995138",
    }

    for sheet_table, gid in db_sheets_gid.items():
        try:
            row_itr = fetch_sheet_lines(document_id, gid)
            create_table(conn, sheet_table, row_itr)
        except Exception as e:
            conn.rollback()
            msg = f"Error creating table '{sheet_table}' from sheet gid = '{gid}'"
            raise Exception(msg) from e

    for old_col, new_col in (
        ("sample", "specimen"),
        ("statussummary", "status_summary"),
    ):
        conn.execute(f"ALTER TABLE status RENAME COLUMN {old_col} TO {new_col}")

    # Split the run_accessions column into an array
    conn.execute(
        """
        CREATE TABLE metagenome AS
        SELECT * EXCLUDE run_accessions
          , string_split(run_accessions, ',') AS run_accessions
        FROM metagenome_tmp
        """
    )
    conn.execute("DROP TABLE metagenome_tmp")

    # Fix metagenome_bin.length which is a FLOAT in Mbp to INTEGER in bp
    conn.execute("UPDATE metagenome_bin SET length = round(1e6 * length, 0)")
    conn.execute("ALTER TABLE metagenome_bin ALTER COLUMN length TYPE INTEGER")
    for col in ("23s", "16s", "5s"):
        conn.execute(f'ALTER TABLE metagenome_bin ALTER COLUMN "{col}" TYPE BOOLEAN')
        conn.execute(f'ALTER TABLE metagenome_bin RENAME COLUMN "{col}" TO has_{col}')

    conn.commit()

    # Start duckdb cli if run in a terminal
    if sys.stdout.isatty():
        conn.close()
        os.execlp("duckdb", "duckdb", "-ui", duckdb_file)  # noqa: S606, S607


def create_table(conn, table_name, row_itr):
    try:
        header = next(row_itr)
    except StopIteration:
        msg = "No header. Empty file?"
        raise ValueError(msg) from None

    # Exclue any leading and trailing columns with an empty header
    first_col, last_col = None, None
    for i, v in enumerate(header):
        if v:
            if first_col is None:
                first_col = i
            last_col = i + 1
    if first_col is None:
        msg = "All header columns are empty. Blank first line in sheet?"
        raise ValueError(msg)

    header = cleanup_header(header[first_col:last_col])

    # Create temporary TSV file and write header and body
    tsv_tmp = tempfile.NamedTemporaryFile(  # noqa: SIM115
        "w",
        prefix=f"status__{table_name}_",
        suffix=".tsv",
    )
    tsv_tmp.write("\t".join(header) + "\n")
    for row in row_itr:
        clean = cleanup_row(row[first_col:last_col])
        tsv_tmp.write("\t".join(clean) + "\n")

    # Ensure data is flushed to storage
    tsv_tmp.flush()
    os.fsync(tsv_tmp.fileno())

    # Import the data from the temporary CSV file into duckdb
    stmt = f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv_auto(?)"  # noqa: S608
    conn.execute(stmt, (tsv_tmp.name,))


def fetch_sheet_lines(document_id, gid):
    url = f"https://docs.google.com/spreadsheets/d/{document_id}/export"
    r = requests.get(url, params={"gid": gid, "format": "tsv"}, timeout=10)
    if r.status_code == requests.codes.ok:
        # Encoding was 'ISO-8859-1'.  Setting to apparent sets 'utf-8',
        # correctly encoding bullet characters in spreadsheet.
        r.encoding = r.apparent_encoding

        for line in r.iter_lines(decode_unicode=True):
            # Skip blank lines
            if not re.search(r"\w", line):
                continue
            yield tuple(line.rstrip("\r\n").split("\t"))
    else:
        r.raise_for_status()


def cleanup_header(dirty):
    clean = tuple(make_identifier(x) for x in dirty)

    # If all column headers are all upper case, convert them to lower case
    all_caps = True
    for col in clean:
        if re.search(r"[^A-Z_]", col):
            all_caps = False
            break

    if all_caps:
        return tuple(x.lower() for x in clean)

    return clean


def make_identifier(txt):
    txt = re.sub(r"&+", " and ", txt)
    txt = re.sub(r"%+", " pct ", txt)
    txt = re.sub(r"\W+", "_", txt)
    return txt.strip("_")


def cleanup_row(dirty):
    return tuple(cleanup_cell(x) for x in dirty)


strip_commas = str.maketrans({",": None})


def cleanup_cell(cell):
    txt = cell.strip()

    if txt == "-":
        return ""

    # Remove commas from numbers. e.g. "1,200" becomes "1200"
    if re.match(r"\d[\d,]+(\.\d+)?$", txt):
        return txt.translate(strip_commas)

    return txt


if __name__ == "__main__":
    cli()
