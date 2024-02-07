import click
import inspect
import json
import logging
import sys
from functools import cache

from tola import db_connection
from tola import tolqc_client
from tola.nd_json import ndjson_row


@click.command()
@tolqc_client.tolqc_url
@tolqc_client.api_token
@click.argument(
    "project_id_list",
    metavar="PROJECT_ID",
    type=click.INT,
    nargs=-1,
    required=True,
)
def cli(tolqc_url, api_token, project_id_list):
    """
    Fetch sequencing data from the Multi-LIMS Warehouse (MLWH)

    Fetches both Illumina and PacBio sequencing run data by querying the MLWH
    MySQL database.

    Where each PROJECT_ID is a numeric project ID,
    e.g. 5901 (Darwin Tree of Life)
    """
    client = tolqc_client.TolClient(tolqc_url, api_token)
    mlwh = db_connection.mlwh_db()
    for project_id in project_id_list:
        for run_data_fetcher in pacbio_fetcher, illumina_fetcher:
            row_itr = run_data_fetcher(mlwh, project_id)
            rspns = client.json_post('loader/seq-data', row_itr)
            print(rspns, file=sys.stderr)


def illumina_fetcher(mlwh, project_id):
    logging.info(f"Fetching Illumina data for project '{project_id}'")
    crsr = mlwh.cursor(dictionary=True)
    crsr.execute(illumina_sql(), (project_id,))
    for row in crsr:
        # Either position (lane) or tag_index (plex) can be NULL
        if name_root := row["run_id"]:
            if row["position"]:
                name_root += "_" + row["position"]
            if row["tag_index"]:
                name_root += "#" + row["tag_index"]
        else:
            msg = row_message(row, "No run_id for row")
            raise ValueError(msg)

        row["name_root"] = name_root
        yield ndjson_row(row)


def pacbio_fetcher(mlwh, project_id):
    logging.info(f"Fetching PacBio data for project '{project_id}'")
    crsr = mlwh.cursor(dictionary=True)
    crsr.execute(pacbio_sql(), (project_id,))
    for row in crsr:
        if name_root := row["run_id"]:
            if row["tag1_id"]:
                name_root += "#" + row["tag1_id"]
                if row["tag2_id"]:
                    name_root += "#" + row["tag2_id"]
            elif row["tag2_id"]:
                msg = row_message(row, "Do not expect tag2_id without tag1_id")
                raise ValueError(msg)
        else:
            # We don't care about missing movie names for failed sequencing
            if row["lims_qc"] == "pass":
                row_info(row, "Missing movie_name")
            continue

        row["name_root"] = name_root
        yield ndjson_row(row)


@cache
def illumina_sql():
    return inspect.cleandoc(
        """
        SELECT study.id_study_lims AS study_id
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
        LEFT JOIN seq_product_irods_locations AS irods
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
        SELECT study.id_study_lims AS study_id
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
        LEFT JOIN seq_product_irods_locations AS irods
          ON product_metrics.id_pac_bio_product = irods.id_product
        WHERE well_metrics.movie_name IS NOT NULL
          AND study.id_lims = 'SQSCP'
          AND study.id_study_lims = %s
        """,
    )


if __name__ == "__main__":
    cli()
