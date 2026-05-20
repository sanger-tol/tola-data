import click
import duckdb
import pyarrow
from tol.core import DataSourceFilter

from tola import click_options
from tola.ndjson import get_input_objects
from tola.tolqc_client import TolClient


@click.command
@click_options.tolqc_alias
@click_options.input_files
def cli(tolqc_alias, input_files):
    """
    Populate the `assembly` table with records fetched from the ENA.
    """

    if input_files:
        search_accessions = search_accessions_from_files(input_files)
    else:
        # Sanger Tree of Life project accession
        search_accessions = ["PRJEB43745"]

    client = TolClient(
        tolqc_alias=tolqc_alias,
        page_size=1000,
    )

    conn = duckdb.connect()
    cache_tolqc_assemblies(client, conn)
    loaded = []
    for accession in search_accessions:
        load_ena_assemblies(client, conn, accession, loaded)


def search_accessions_from_files(input_files) -> list[str]:
    """
    Extracts a list of accessions from ND-JSON input files.
    """
    search_acc = []
    for obj in get_input_objects(input_files):
        acc = obj.get("accession.id") or obj.get("accession")
        if acc:
            search_acc.append(acc)
    return search_acc


def cache_tolqc_assemblies(client: TolClient, conn: duckdb.DuckDBPyConnection):
    filt = DataSourceFilter(
        and_={
            "genome_accession.id": {"exists": {"negate": True}},
        }
    )

    genome_acc = []
    asm_id = []
    for asm in client.ads_ro.get_list(
        "assembly",
        object_filters=filt,
        requested_fields=["id"],
    ):
        genome_acc.append(asm.genome_accession.id)
        asm_id.append(asm.id)

    loaded_asm = pyarrow.Table.from_pydict(  # noqa: F841
        {
            "genome_accession_id": genome_acc,
            "assembly_id": asm_id,
        }
    )

    conn.execute("""
      CREATE TABLE loaded_assemblies
      AS FROM loaded_asm
      ORDER BY genome_accession_id
    """)


def load_ena_assemblies(
    client: TolClient,
    conn: duckdb.DuckDBPyConnection,
    accession: str,
    loaded: list,
):

    ena_fields = {
        "genome_accession_id": "assembly_set_accession",
        "bioproject_accession_id": "study_accession",
        "name": "assembly_name",
        "description": "assembly_title",
        "level": "assembly_level",
        "status": "status",
        "last_updated": "last_updated",
    }

    # Build ENA filereport query URL
    params = "&".join(
        f"{k}={v}"
        for k, v in {
            "accession": accession,
            "result": "assembly",
            "fields": ",".join(ena_fields.values()),
            "format": "CSV",
        }.items()
    )
    filereport_url = f"https://www.ebi.ac.uk/ena/portal/api/filereport?{params}"

    # Get new insdc_path values to load by joining pyarrow table to result
    # from ENA query.
    column_names = ", ".join(f"{col} AS {alias}" for alias, col in ena_fields.items())
    try:
        conn.execute(
            f"""
            WITH ena AS (
              FROM read_csv(?)
            )
            SELECT
              {column_names}
            FROM ena
            ANTI JOIN loaded_assemblies
              ON ena.assembly_set_accession = loaded_assemblies.genome_accession_id
            """,  # noqa: S608
            [filereport_url],
        )
        new_asm = conn.fetch_arrow_table()
    except duckdb.BinderException:
        # Empty result for accession
        return

    # Store any new insdc_path values
    ads = client.ads
    cdo = client.build_cdo
    for batch in new_asm.to_batches(client.page_size):
        gen_acc_ver = batch.column("genome_accession_id")
        bioproject_acc = batch.column("bioproject_accession_id")

        for i in range(batch.num_rows):
            print(f"{gen_acc_ver[i]}\t{bioproject_acc[i]}")

        # file_upd = []
        # for i in range(batch.num_rows):
        #     file_upd.append(cdo("file", ids[i], {"insdc_path": ftp[i].as_py()}))
        # for file in ads.upsert("file", file_upd):
        #     if isinstance(file, ErrorObject):
        #         err = file
        #         upsert_rslt.append(
        #             {
        #                 f"{err.object_type}.id": err.object_id,
        #                 "error": err.details,
        #                 "object": err.object_,
        #             }
        #         )
        #     else:
        #         upsert_rslt.append(
        #             {
        #                 "data.id": file.data.id,
        #                 "insdc_path": file.insdc_path,
        #             }
        #         )
