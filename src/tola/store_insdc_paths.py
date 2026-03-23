import sys

import click
import duckdb
import pyarrow
from tol.core import DataSourceFilter, ErrorObject

from tola import click_options
from tola.pretty import plain_text_from_itr
from tola.terminal import colour_pager, pretty_dict_itr
from tola.tolqc_client import TolClient


@click.command
@click_options.tolqc_alias
def cli(tolqc_alias):
    """
    Populates `file.insdc_path` from the ENA filereport endpoint by matching
    md5 values.
    """
    page_size = 1000
    client = TolClient(
        tolqc_alias=tolqc_alias,
        page_size=page_size,
    )

    # Make a pyarrow table file.id and md5 for all file table rows missing the
    # insdc_path.
    missing_build = {
        "file_id": [],
        "md5": [],
    }
    for file in client.ads.get_list(
        "file",
        object_filters=DataSourceFilter(
            and_={"insdc_path": {"eq": {"value": None}}},
        ),
        requested_fields=["md5"],
    ):
        missing_build["file_id"].append(file.id)
        missing_build["md5"].append(file.md5)
    missing_insdc = pyarrow.Table.from_pydict(missing_build)  # noqa: F841

    # Build ENA filereport query URL
    params = "&".join(
        f"{k}={v}"
        for k, v in {
            "accession": "PRJEB43745",  # Sanger Tree of Life
            "result": "read_run",
            "fields": "submitted_md5,submitted_ftp",
            "format": "json",
        }.items()
    )
    filereport_url = f"https://www.ebi.ac.uk/ena/portal/api/filereport?{params}"

    # Get new insdc_path values to load by joining pyarrow table to result
    # from ENA query.
    conn = duckdb.connect()
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
    upsert_rslt = []
    for batch in new_ftp.to_batches(page_size):
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

    if upsert_rslt:
        # Pretty print the new insdc_path entries
        itr = pretty_dict_itr(upsert_rslt, "data.id", head="Filled in {} new insdc_path{}:")
        if sys.stdout.isatty():
            colour_pager(itr)
        else:
            print(plain_text_from_itr(itr))


if __name__ == "__main__":
    cli()
