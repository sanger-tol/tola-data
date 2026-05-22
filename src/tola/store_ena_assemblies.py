import sys

import click
import duckdb

from tola import click_options
from tola.ndjson import get_input_objects, ndjson_row
from tola.pretty import plain_text_from_itr
from tola.terminal import colour_pager, pretty_dict_itr
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

    if loaded:
        # Pretty print the new ENA assembly entries
        itr = pretty_dict_itr(loaded, None, head="Stored {} new ENA assembly record{}:")
        if sys.stdout.isatty():
            colour_pager(itr)
        else:
            print(plain_text_from_itr(itr))


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
    sql = """
      CREATE TABLE tolqc AS
      FROM
        read_json(
          ?,
          columns := {
            specimen_biosample: 'VARCHAR',
            specimen: 'VARCHAR',
            assembly_bioproject: 'VARCHAR',
            genome_accession_id: 'VARCHAR',
            name: 'VARCHAR',
            description: 'VARCHAR',
            level: 'VARCHAR',
            status: 'VARCHAR',
            status_time: 'DATE',
          }
        );
    """
    url = client.report_url(
        "ena-assembly",
        params={"format": "NDJSON"},
    )
    conn.execute(sql, [url])


def load_ena_assemblies(
    client: TolClient,
    conn: duckdb.DuckDBPyConnection,
    accession: str,
    loaded: list,
):
    ena_fields = {
        "specimen_biosample": "sample_accession",
        "genome_accession_id": "assembly_set_accession",
        "assembly_bioproject": "study_accession",
        "name": "assembly_name",
        "description": "assembly_title",
        "level": "assembly_level",
        "status": "status",
        "status_time": "last_updated",
        # "run_accession_list": "run_accession",
    }

    # Build ENA filereport query URL
    params = "&".join(
        f"{k}={v}"
        for k, v in {
            "result": "assembly",
            "format": "CSV",
            "accession": accession,
            "fields": ",".join(ena_fields.values()),
        }.items()
    )
    filereport_url = f"https://www.ebi.ac.uk/ena/portal/api/filereport?{params}"

    # # Split run_accession_list string into a list
    # ena_fields["run_accession_list"] = "string_split(run_accession, ';')"

    # Get new ENA assembly records by joining to the cached tolqc table
    column_specs = ",\n          ".join(
        f"{col} AS {alias}" for alias, col in ena_fields.items()
    )

    # Filter results from ENA by those which have a matching BioSample
    # accession in the ToLQC specimen table, then look for any GCA accessions
    # which aren't in the assembly table.
    sql = f"""
      WITH
        ena AS (
          SELECT
            {column_specs}
          FROM
            read_csv(?)
        ),
        sbs AS (
          SELECT DISTINCT
            specimen_biosample,
            tolqc.specimen
          FROM
            ena
            JOIN tolqc USING (specimen_biosample)
        )
      SELECT
        sbs.specimen,
        ena.*
      FROM
        ena
        JOIN sbs USING (specimen_biosample)
        LEFT JOIN tolqc USING (genome_accession_id)
      WHERE
        tolqc.genome_accession_id IS NULL
      ORDER BY
        specimen,
        name
    """  # noqa: S608

    try:
        conn.execute(sql, [filereport_url])
    except duckdb.BinderException:
        # Empty result for accession
        return

    # Mapping of ENA status names to our `assembly_status_id`
    status_names = {"public": "ENA Public"}

    ads = client.ads
    cdo = client.build_cdo
    for arrow_batch in conn.to_arrow_reader(client.page_size):
        batch = arrow_batch.to_pydict()
        accession_cdo = []
        assembly_cdo = []
        statuses = {}
        for i in range(arrow_batch.num_rows):
            # GenBank assembly accession
            acc_sv = batch["genome_accession_id"][i]
            accession_cdo.append(
                cdo(
                    "accession",
                    acc_sv,
                    {"accession_type_id": "GenBank Genome Assembly"},
                )
            )

            # Assembly BioProject accession
            bio_acc = batch["assembly_bioproject"][i]
            accession_cdo.append(
                cdo(
                    "accession",
                    bio_acc,
                    {"accession_type_id": "BioProject - Species Assembly"},
                )
            )

            # Assembly table entry
            assembly_cdo.append(
                cdo(
                    "assembly",
                    None,
                    {
                        "genome_accession_id": acc_sv,
                        "bioproject_accession_id": bio_acc,
                        "specimen_id": batch["specimen"][i],
                        "name": batch["name"][i],
                        "description": batch["description"][i],
                        "level": batch["level"][i],
                    },
                )
            )
            status = batch["status"][i]

            statuses[acc_sv] = {
                "status_type.id": status_names.get(status, status),
                "status_time": batch["status_time"][i],
            }

            loaded.append(
                {
                    "specimen.id": batch["specimen"][i],
                    "name": batch["name"][i],
                    "bioproject_accession.id": bio_acc,
                    "genome_accession.id": acc_sv,
                }
            )

        # Use `upsert()` to insert the accessions in case any are already
        # loaded.
        ads.upsert("accession", accession_cdo)

        # Insert will always work because we're
        assemblies = ads.insert("assembly", assembly_cdo)
        status_json = []
        for asm in assemblies:
            acc_sv = asm.genome_accession.id
            status = statuses[acc_sv]
            status_json.append(ndjson_row({"assembly.id": asm.id, **status}))
        client.ndjson_post("loader/status/assembly", status_json)
