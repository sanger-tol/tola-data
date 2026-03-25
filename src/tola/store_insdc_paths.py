import sys

import click
import duckdb
import pyarrow
from tol.core import DataSourceFilter, ErrorObject

from tola import click_options
from tola.ndjson import get_input_objects
from tola.pretty import plain_text_from_itr
from tola.terminal import colour_pager, pretty_dict_itr
from tola.tolqc_client import TolClient


@click.command
@click_options.tolqc_alias
@click_options.input_files
@click.option(
    "--from-run-accessions",
    flag_value=True,
    default=False,
    show_default=True,
    help="""
      Use `data.accession_id` run accessions to fill in `file.indsc_path`.
      This is will redundantly query files which are never submitted, once
      per file, so should only be run occasionally.
    """,
)
def cli(tolqc_alias, input_files, from_run_accessions):
    """
    Populates `file.insdc_path` from the ENA filereport endpoint by matching
    md5 values.

    Looks for `accession.id`, `accession` or `run_accession` keys in the
    ND-JSON in the INPUT_FILES.  Defaults to fetching accessions for all
    """

    if from_run_accessions:
        search_accessions = []
    elif input_files:
        search_accessions = search_accessions_from_files(input_files)
    else:
        search_accessions = ["PRJEB43745"]

    page_size = 1000
    client = TolClient(
        tolqc_alias=tolqc_alias,
        page_size=page_size,
    )

    # Make a pyarrow table file.id and md5 for all file table rows missing the
    # insdc_path.
    miss_file_id = []
    miss_md5 = []
    miss_run_accession = []
    for file in client.ads.get_list(
        "file",
        object_filters=DataSourceFilter(
            and_={"insdc_path": {"eq": {"value": None}}},
        ),
        requested_fields=["md5", "data.id", "data.accession.id"],
    ):
        miss_file_id.append(file.id)
        miss_md5.append(file.md5)
        miss_run_accession.append(acc.id if (acc := file.data.accession) else None)
    missing_insdc = pyarrow.Table.from_pydict(  # noqa: F841
        {
            "file_id": miss_file_id,
            "md5": miss_md5,
            "run_accession": miss_run_accession,
        }
    )

    conn = duckdb.connect()

    if from_run_accessions:
        conn.execute("""
          SELECT run_accession
          FROM missing_insdc
          WHERE run_accession IS NOT NULL
        """)
        search_accessions = [acc for (acc,) in conn.fetchall()]

    upsert_rslt = []
    for acc in search_accessions:
        fetch_ebi_filereport_data(conn, client, missing_insdc, upsert_rslt, acc)

    if upsert_rslt:
        # Pretty print the new insdc_path entries
        itr = pretty_dict_itr(
            upsert_rslt, "data.id", head="Filled in {} new insdc_path{}:"
        )
        if sys.stdout.isatty():
            colour_pager(itr)
        else:
            print(plain_text_from_itr(itr))


def search_accessions_from_files(input_files) -> list[str]:
    search_acc = []
    for obj in get_input_objects(input_files):
        acc = (
            obj.get("accession.id") or obj.get("accession") or obj.get("run_accession")
        )
        if acc:
            search_acc.append(acc)
    return search_acc


def fetch_ebi_filereport_data(conn, client, missing_insdc, upsert_rslt, accession):

    # Build ENA filereport query URL
    params = "&".join(
        f"{k}={v}"
        for k, v in {
            "accession": accession,  # Sanger Tree of Life
            "result": "read_run",
            "fields": "submitted_md5,submitted_ftp",
            "format": "json",
        }.items()
    )
    filereport_url = f"https://www.ebi.ac.uk/ena/portal/api/filereport?{params}"

    # Get new insdc_path values to load by joining pyarrow table to result
    # from ENA query.
    conn.execute(
        """
        WITH ena AS (
          SELECT
            submitted_md5.regexp_split_to_table(';') AS md5,
            submitted_ftp.regexp_split_to_table(';') AS ftp
          FROM read_json(?)
        )
        SELECT
          file_id,
          concat('https://', replace(ftp, '#', '%23')) AS ftp
        FROM ena
        JOIN missing_insdc USING (md5)
        """,
        [filereport_url],
    )
    new_ftp = conn.fetch_arrow_table()

    # Store any new insdc_path values
    ads = client.ads
    cdo = client.build_cdo
    for batch in new_ftp.to_batches(client.page_size):
        ids = batch.column("file_id")
        ftp = batch.column("ftp")
        file_upd = []
        for i in range(batch.num_rows):
            file_upd.append(cdo("file", ids[i], {"insdc_path": ftp[i].as_py()}))
        for file in ads.upsert("file", file_upd):
            if isinstance(file, ErrorObject):
                err = file
                upsert_rslt.append(
                    {
                        f"{err.object_type}.id": err.object_id,
                        "error": err.details,
                        "object": err.object_,
                    }
                )
            else:
                upsert_rslt.append(
                    {
                        "data.id": file.data.id,
                        "insdc_path": file.insdc_path,
                    }
                )


if __name__ == "__main__":
    cli()
