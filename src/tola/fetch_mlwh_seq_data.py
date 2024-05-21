import click
import inspect
import logging
import re
import sys
from functools import cache
from io import StringIO

from tol.core import DataSourceFilter
from tola import db_connection, tolqc_client
from tola.goat_client import GoaTClient
from tola.ndjson import ndjson_row


@click.command()
@tolqc_client.tolqc_url
@tolqc_client.api_token
@tolqc_client.tolqc_alias
@click.option(
    "--stdout/--server",
    "write_to_stdout",
    default=False,
    show_default=True,
    help="""
    Writes the fetched MLWH data to STDOUT as NDJSON instead of saving to the
    ToLQC database.
    """,
)
@click.argument(
    "project_id_list",
    metavar="PROJECT_ID",
    type=click.INT,
    nargs=-1,
    required=False,
)
def cli(tolqc_url, api_token, tolqc_alias, project_id_list, write_to_stdout):
    """
    Fetch sequencing data from the Multi-LIMS Warehouse (MLWH)

    Fetches both Illumina and PacBio sequencing run data by querying the MLWH
    MySQL database, and prints out a report of new and changed data.

    Where each PROJECT_ID is a numeric project ID,
    e.g. 5901 (Darwin Tree of Life).

    Iterates over each project in the ToLQC database if no PROJECT_IDs are
    supplied.
    """
    client = tolqc_client.TolClient(tolqc_url, api_token, tolqc_alias)
    if not project_id_list:
        project_id_list = client.list_project_lims_ids()
    mlwh = db_connection.mlwh_db()
    for project_id in project_id_list:
        for platform, run_data_fetcher in (
            ("PacBio", pacbio_fetcher),
            ("Illumina", illumina_fetcher),
        ):
            row_itr = run_data_fetcher(mlwh, project_id)
            if write_to_stdout:
                for row in row_itr:
                    sys.stdout.write(row)
            else:
                rspns = chunk_requests(client, row_itr)
                print(formatted_response(rspns, project_id, platform), end="")
    if not write_to_stdout:
        patch_species(client)


def chunk_requests(client, row_itr):
    max_request_size = 2 * 1024**2
    merged_rspns = {}
    for chunk in chunk_rows(row_itr, max_request_size):
        rspns = client.json_post("loader/seq-data", chunk)
        merge_response(merged_rspns, rspns)
    return merged_rspns


def chunk_rows(row_itr, max_request_size):
    chunk = StringIO()
    for row in row_itr:
        if chunk.tell() + len(row) > max_request_size:
            yield chunk.getvalue()
            chunk.seek(0)
            chunk.truncate(0)
        chunk.write(row)

    yield chunk.getvalue()


def merge_response(merged, rspns):
    for x in rspns:
        mrg = merged.setdefault(x, [])
        mrg.extend(rspns[x])


def formatted_response(response, project_id, platform):
    out = StringIO("")

    new = response.get("new")
    if new:
        out.write(
            f"\n\nNew {platform} data in '{new[0]['project']} ({project_id})':\n\n"
        )
        for row in new:
            out.write(response_row_std_fields(row))

    upd = response.get("updated")
    if upd:
        out.write(
            f"\n\nUpdated {platform} data in '{upd[0]['project']} ({project_id})':\n\n"
        )
        for row in upd:
            out.write(response_row_std_fields(row))
            for col, old_new in row["changes"].items():
                out.write(f"  {col} changed from {old_new[0]!r} to {old_new[1]!r}\n")

    return out.getvalue()


def response_row_std_fields(row):
    return "\t".join(str(row[x]) for x in row if x not in ("project", "changes")) + "\n"


def illumina_fetcher(mlwh, project_id):
    logging.info(f"Fetching Illumina data for project '{project_id}'")
    crsr = mlwh.cursor(dictionary=True)
    crsr.execute(illumina_sql(), (project_id,))
    for row in crsr:
        yield ndjson_row(row)


def pacbio_fetcher(mlwh, project_id):
    logging.info(f"Fetching PacBio data for project '{project_id}'")
    crsr = mlwh.cursor(dictionary=True)
    crsr.execute(pacbio_sql(), (project_id,))
    for row in crsr:
        name = row["name_root"]
        tag1 = trimmed_tag(row["tag1_id"])
        tag2 = trimmed_tag(row["tag2_id"])
        if tag2:
            row["name_root"] = f"{name}#{tag1}#{tag2}"
        elif tag1:
            row["name_root"] = f"{name}#{tag1}"
        yield ndjson_row(row)


def trimmed_tag(tag):
    """The same tag in PacBio data can appear as:

         "bc1008"

         "bc1008_BAK8A_OA"

         "1008"

    This functions trims them to the four (or more) digit form.
    """
    if tag is None:
        return tag

    if m := re.match(r"bc(\d{4,})", tag):
        return m.group(1)
    else:
        return tag


@cache
def illumina_sql():
    return inspect.cleandoc(
        """
        SELECT REGEXP_REPLACE(
            -- Trim file suffix, i.e. ".cram"
            irods.irods_data_relative_path
              , '\\.[[:alnum:]]+$'
              , ''
            ) AS name_root
          , study.id_study_lims AS study_id
          , sample.name AS sample_name
          , sample.supplier_name AS supplier_name
          , sample.public_name AS tol_specimen_id
          , sample.accession_number AS biosample_accession
          , sample.donor_id AS biospecimen_accession
          , sample.common_name AS scientific_name
          , sample.taxon_id AS taxon_id
          , 'Illumina' AS platform_type
          , run_lane_metrics.instrument_model AS instrument_model
          , run_lane_metrics.instrument_name AS instrument_name
          , flowcell.pipeline_id_lims AS pipeline_id_lims
          , CONVERT(product_metrics.id_run, CHAR) AS run_id
          , CONVERT(product_metrics.position, CHAR) AS position
          , CONVERT(product_metrics.tag_index, CHAR) AS tag_index
          , run_lane_metrics.run_complete AS run_complete
          , IF(product_metrics.qc IS NULL, NULL
            , IF(product_metrics.qc = 1, 'pass', 'fail')) AS lims_qc
          , run_lane_metrics.qc_complete AS qc_date
          , flowcell.tag_identifier AS tag1_id
          , flowcell.tag2_identifier AS tag2_id
          , flowcell.id_library_lims AS library_id
          , irods.irods_root_collection AS irods_path
          , irods.irods_data_relative_path AS irods_file
        FROM study
        JOIN iseq_flowcell AS flowcell
          ON study.id_study_tmp = flowcell.id_study_tmp
        JOIN sample
          ON flowcell.id_sample_tmp = sample.id_sample_tmp
        JOIN iseq_product_metrics AS component_metrics
          ON flowcell.id_iseq_flowcell_tmp = component_metrics.id_iseq_flowcell_tmp
        JOIN iseq_run_lane_metrics AS run_lane_metrics
          ON component_metrics.id_run = run_lane_metrics.id_run
          AND component_metrics.position = run_lane_metrics.position
        JOIN iseq_product_components AS components
          ON component_metrics.id_iseq_pr_metrics_tmp
                  = components.id_iseq_pr_component_tmp
          AND components.component_index = 1
        JOIN iseq_product_metrics AS product_metrics
          ON components.id_iseq_pr_tmp = product_metrics.id_iseq_pr_metrics_tmp
        JOIN seq_product_irods_locations AS irods
          ON product_metrics.id_iseq_product = irods.id_product
        WHERE run_lane_metrics.qc_complete IS NOT NULL
          AND product_metrics.num_reads IS NOT NULL
          AND study.id_lims = 'SQSCP'
          AND study.id_study_lims = %s
        """,
    )


@cache
def pacbio_sql():
    return inspect.cleandoc(
        """
        WITH plex_agg AS (
          SELECT rwm.id_pac_bio_rw_metrics_tmp
            , COUNT(*) plex_count
          FROM pac_bio_run_well_metrics rwm
          JOIN pac_bio_product_metrics pm
            USING (id_pac_bio_rw_metrics_tmp)
          GROUP BY rwm.id_pac_bio_rw_metrics_tmp
        )
        SELECT well_metrics.movie_name AS name_root
          , study.id_study_lims AS study_id
          , sample.name AS sample_name
          , sample.supplier_name AS supplier_name
          , sample.public_name AS tol_specimen_id
          , sample.accession_number AS biosample_accession
          , sample.donor_id AS biospecimen_accession
          , sample.common_name AS scientific_name
          , sample.taxon_id AS taxon_id
          , 'PacBio' AS platform_type
          , REGEXP_REPLACE(instrument_type
            , '^Sequel2', 'Sequel II') AS instrument_model
          , CONCAT('m', LOWER(instrument_name)) AS instrument_name
          , run.pipeline_id_lims AS pipeline_id_lims
          , well_metrics.movie_name AS run_id
          , well_metrics.pac_bio_run_name AS lims_run_id
          , well_metrics.well_label AS well_label
          , well_metrics.run_start AS run_start
          , well_metrics.run_complete AS run_complete
          , plex_agg.plex_count AS plex_count
          , IF(well_metrics.qc_seq IS NULL, NULL
            , IF(well_metrics.qc_seq = 1, 'pass', 'fail')) AS lims_qc
          , well_metrics.qc_seq_date AS qc_date
          , run.tag_identifier AS tag1_id
          , run.tag2_identifier AS tag2_id
          , run.pac_bio_library_tube_name AS library_id

          -- Fields for PacbioRunMetrics:
          , well_metrics.movie_minutes AS movie_minutes
          , well_metrics.binding_kit AS binding_kit
          , well_metrics.sequencing_kit AS sequencing_kit
          , IF(well_metrics.include_kinetics IS NULL, NULL,
              IF(well_metrics.include_kinetics = 1, 'true', 'false')
            ) AS include_kinetics
          , well_metrics.loading_conc AS loading_conc
          , well_metrics.control_num_reads AS control_num_reads
          , well_metrics.control_read_length_mean AS control_read_length_mean
          , well_metrics.polymerase_read_bases AS polymerase_read_bases
          , well_metrics.polymerase_num_reads AS polymerase_num_reads
          , well_metrics.polymerase_read_length_mean AS polymerase_read_length_mean
          , well_metrics.polymerase_read_length_n50 AS polymerase_read_length_n50
          , well_metrics.insert_length_mean AS insert_length_mean
          , well_metrics.insert_length_n50 AS insert_length_n50
          , well_metrics.unique_molecular_bases AS unique_molecular_bases
          , well_metrics.p0_num AS p0_num
          , well_metrics.p1_num AS p1_num
          , well_metrics.p2_num AS p2_num
          , well_metrics.hifi_read_bases AS hifi_read_bases
          , well_metrics.hifi_num_reads AS hifi_num_reads
          , well_metrics.hifi_low_quality_num_reads AS hifi_low_quality_num_reads

          , irods.irods_root_collection AS irods_path
          , irods.irods_data_relative_path AS irods_file
        FROM study
        JOIN pac_bio_run AS run
          ON study.id_study_tmp = run.id_study_tmp
        JOIN sample
          ON run.id_sample_tmp = sample.id_sample_tmp
        JOIN pac_bio_product_metrics AS product_metrics
          ON run.id_pac_bio_tmp = product_metrics.id_pac_bio_tmp
        JOIN pac_bio_run_well_metrics AS well_metrics
          ON product_metrics.id_pac_bio_rw_metrics_tmp
              = well_metrics.id_pac_bio_rw_metrics_tmp
        JOIN plex_agg
          ON well_metrics.id_pac_bio_rw_metrics_tmp
               = plex_agg.id_pac_bio_rw_metrics_tmp
        JOIN seq_product_irods_locations AS irods
          ON product_metrics.id_pac_bio_product = irods.id_product
        WHERE product_metrics.qc IS NOT NULL
          AND well_metrics.movie_name IS NOT NULL
          AND study.id_lims = 'SQSCP'
          AND study.id_study_lims = %s
        """,
    )


def patch_species(client):
    ads = client.ads
    obj_factory = ads.data_object_factory
    filt = DataSourceFilter(
        exact={
            "taxon_family": None,
            "taxon_order": None,
            "taxon_phylum": None,
            "taxon_group": None,
        }
    )
    gc = GoaTClient()
    updates = []
    for sp in client.ads.get_list("species", object_filters=filt):
        if spec_info := gc.get_species_info(sp.taxon_id):
            if sp.id != spec_info["species_id"]:
                logging.info(
                    f"Species with taxon_id = '{sp.taxon_id}' should be"
                    f" named '{spec_info['species_id']}' not '{sp.id}'"
                )
            changes = {}
            for prop, val in spec_info.items():
                if prop == "species_id":
                    continue
                if getattr(sp, prop) is None:
                    changes[prop] = val
            if changes:
                updates.append(
                    obj_factory("species", id_=sp.id, attributes=changes)
                )
    for page in client.pages(updates):
        ads.upsert("species", page)


if __name__ == "__main__":
    cli()
