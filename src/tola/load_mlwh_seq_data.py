import inspect
import logging
import re
import sys
from functools import cache

from tola.goat_client import GoaTClient
from main.model import (
    Accession,
    Allocation,
    Centre,
    Data,
    File,
    PacbioRunMetrics,
    Platform,
    Run,
    Sample,
    Species,
    Specimen,
)

import tola.marshals
from tola import db_connection

logging.basicConfig(level=logging.INFO)


def main():
    mrshl, _ = tola.marshals.marshal_from_command_line(
        "Import sequencing run data from the MLWH",
    )
    mlwh = db_connection.mlwh_db()
    sts = db_connection.sts_db()
    load_mlwh_data(mrshl, mlwh, sts)
    mrshl.commit()


def isoformat_if_date(dt):
    return dt.isoformat() if hasattr(dt, "isoformat") else dt


def load_mlwh_data(mrshl, mlwh, sts):
    centre = mrshl.fetch_one(Centre, {"name": "Wellcome Sanger Institute"}, ("name",))
    goat_client = GoaTClient()
    # Iterate through projects
    for project in mrshl.list_projects():
        for run_data_fetcher in illumina_fetcher, pacbio_fetcher:
            for row in run_data_fetcher(mlwh, project):
                try:
                    store_row_data(
                        mrshl,
                        centre,
                        row,
                        goat_client,
                        project,
                    )
                except Exception as err:
                    msg = f"Error saving row:\n{format_row(row)}\n{err}"
                    raise ValueError(msg) from err


PACBIO_RUN_METRICS_FIELDS = (
    "run_id",
    "movie_minutes",
    "binding_kit",
    "sequencing_kit",
    "include_kinetics",
    "loading_conc",
    "control_num_reads",
    "control_read_length_mean",
    "polymerase_read_bases",
    "polymerase_num_reads",
    "polymerase_read_length_mean",
    "polymerase_read_length_n50",
    "insert_length_mean",
    "insert_length_n50",
    "unique_molecular_bases",
    "p0_num",
    "p1_num",
    "p2_num",
    "hifi_read_bases",
    "hifi_num_reads",
    "hifi_low_quality_num_reads",
)


def store_row_data(mrshl, centre, row, goat_client, project):
    # Species
    species = mrshl.fetch_one_or_none(
        Species, {"taxon_id": row["taxon_id"]}, ("taxon_id",)
    )
    if not species:
        if species_info := goat_client.get_species_info(row["taxon_id"]):
            species = mrshl.update_or_create(Species, species_info, ("taxon_id",))

    # specimen Accession
    specimen_acc = None
    if row["biospecimen_accession"]:
        specimen_acc = mrshl.update_or_create(
            Accession,
            {
                "accession_id": row["biospecimen_accession"],
                "accession_type_id": "Bio Sample",
            },
        )

    # Specimen
    specimen = None
    if row["tol_specimen_id"]:
        specimen_spec = {
            "specimen_id": row["tol_specimen_id"],
        }
        if species:
            specimen_spec["species_id"] = species.species_id
        if specimen_acc:
            specimen_spec["accession_id"] = specimen_acc.accession_id
        specimen = mrshl.update_or_create(Specimen, specimen_spec)

    # sample Accession
    sample_acc = None
    if row["biosample_accession"]:
        sample_acc = mrshl.update_or_create(
            Accession,
            {
                "accession_id": row["biosample_accession"],
                "accession_type_id": "Bio Sample",
            },
        )

    # Sample
    sample_spec = {
        "sample_id": row["sample_name"],
        "specimen_id": row["tol_specimen_id"],
    }
    if specimen:
        sample_spec["specimen_id"] = specimen.specimen_id
    if sample_acc:
        sample_spec["accession_id"] = sample_acc.accession_id
    sample = mrshl.update_or_create(Sample, sample_spec)

    # Platform
    platform = mrshl.fetch_or_create(
        Platform,
        {
            "name": row["platform_type"],
            "model": row["instrument_model"],
        },
        ("name", "model"),
    )

    # Run
    run = None
    if row["run_id"]:
        run_spec = {
            "run_id": row["run_id"],
            "platform_id": platform.id,
            "centre_id": centre.id,
            "start": isoformat_if_date(row.get("run_start")),
            "complete": isoformat_if_date(row["run_complete"]),
        }
        if lrid := row.get("lims_run_id"):
            run_spec["lims_id"] = lrid
        if elmnt := row.get("well_label"):
            run_spec["element"] = elmnt
        run = mrshl.update_or_create(Run, run_spec)

    # PacbioRunMetrics
    if platform.name == "PacBio":
        mrshl.update_or_create(
            PacbioRunMetrics,
            {col: row[col] for col in PACBIO_RUN_METRICS_FIELDS},
        )

    # Data
    data_spec = {
        "name_root": row["name_root"],
        "sample_id": sample.sample_id,
        "tag_index": row.get("tag_index"),
        "tag1_id": row["tag1_id"],
        "tag2_id": row["tag2_id"],
        "lims_qc": row["lims_qc"],
        "date": isoformat_if_date(row["qc_date"]),
    }
    if run:
        data_spec["run_id"] = run.run_id
    data = mrshl.update_or_create(Data, data_spec, ("name_root",))

    # Allocation
    mrshl.update_or_create(
        Allocation,
        {
            "project_id": project.project_id,
            "data_id": data.data_id,
        },
        ("project_id", "data_id"),
    )

    # File
    if row["irods_path"]:
        # irods_path may have a trailing "/"
        remote_path = row["irods_path"].rstrip("/") + "/" + row["irods_file"]
        mrshl.update_or_create(
            File,
            {
                "data_id": data.data_id,
                "remote_path": remote_path,
            },
            ("data_id",),
        )


def minimal_species_info(taxon_id, sci_name):
    hierarchy_name = re.sub(r"\s+", "_", sci_name)
    return {
        "species_id": sci_name,
        "taxon_id": taxon_id,
        "hierarchy_name": hierarchy_name,
    }


def illumina_fetcher(mlwh, project):
    crsr = mlwh.cursor(dictionary=True)
    crsr.execute(illumina_sql(), (project.lims_id,))
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
        yield row


def pacbio_fetcher(mlwh, project):
    crsr = mlwh.cursor(dictionary=True)
    crsr.execute(pacbio_sql(), (project.lims_id,))
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
        yield row


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
        FROM sample
        JOIN iseq_flowcell AS flowcell
          ON sample.id_sample_tmp = flowcell.id_sample_tmp
        JOIN study
          ON flowcell.id_study_tmp = study.id_study_tmp
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
          AND sample.taxon_id IS NOT NULL
          AND product_metrics.num_reads IS NOT NULL
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
          , well_metrics.instrument_type AS instrument_model
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
        FROM sample
        JOIN pac_bio_run AS run
          ON sample.id_sample_tmp = run.id_sample_tmp
        JOIN pac_bio_product_metrics AS product_metrics
          ON run.id_pac_bio_tmp = product_metrics.id_pac_bio_tmp
        JOIN pac_bio_run_well_metrics AS well_metrics
          ON product_metrics.id_pac_bio_rw_metrics_tmp
              = well_metrics.id_pac_bio_rw_metrics_tmp
        JOIN study
          ON run.id_study_tmp = study.id_study_tmp
        LEFT JOIN seq_product_irods_locations AS irods
          ON product_metrics.id_pac_bio_product = irods.id_product
        WHERE product_metrics.qc IS NOT NULL
          AND sample.taxon_id IS NOT NULL
          AND well_metrics.movie_name IS NOT NULL
          AND study.id_study_lims = %s
        """,
    )


def row_info(row, msg):
    print(row_message(row, msg), file=sys.stderr)


def row_message(row, msg):
    return f"{msg}:\n{format_row(row)}"


def format_row(row):
    name_max = max(len(name) for name in row)
    return "".join(
        f"  {name:>{name_max}} = {'' if row[name] is None else row[name]}\n"
        for name in row
    )


if __name__ == "__main__":
    main()
