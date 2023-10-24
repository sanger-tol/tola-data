import click
import csv
import datetime
import duckdb
import io
import pathlib
import requests


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
    db_sheets = {
        "Status": "status",
        "ASG": "asg",
        "Cobiont Submission": "cobiont_sub",
        "Primary Metagenome Submission": "primary_meta_sub",
        "Binned Metagenome Submission": "binned_meta_sub",
        "Raw data submission": "raw_data_sub",
        "BioProjects": "bio_projects",
    }

    # ID of "Tree of Life assembly informatics" Google Sheet
    document_id = "1RKubj10g13INd4W7alHkwcSVX_0CRvNq0-SRe21m-GM"

    for sheet_name, sheet_table in db_sheets.items():
        data = fetch_sheet_io(document_id, sheet_name)


def fetch_sheet_io(document_id, sheet_name):
    url = f"https://docs.google.com/spreadsheets/d/{document_id}/gviz/tq"
    r = requests.get(url, params={"sheet": sheet_name, "tqx": r"out:csv"})
    if r.status_code == requests.codes.ok:
        return io.StringIO(r.text)
    else:
        r.raise_for_status()


if __name__ == "__main__":
    cli()
