import inspect
import os
import pwd

from . import db_connection
from main.model import Allocation, Data, File, Project, Run, Sample, Specimen


def main():
    engine, Session = db_connection.local_postgres_engine(echo=True)
    mlwh = db_connection.mlwh_db()

    user_name = user_name()

    with Session() as ssn:
        for get_sql in illumina_sql, pacbio_sql:
            ### Iterate through projects ###
            crsr = mlwh.cursor(dictionary=True)
            crsr.execute(get_sql(), (5901,))
            for row in crsr:
                data = Data(
                    name_root=row["name_root"],
                    # tag_index=row["tag_index"],
                    tag1_sequence=row["tag1_id"],
                    tag2_sequence=row["tag2_id"],
                    lims_qc=row["lims_qc"],
                    date=row["qc_date"],
                )
                ssn.merge(data)
                if row["irods_path"]:
                    file = File(
                        data_id=data.data_id,
                        name=row["irods_file"],
                        remote_path=row["irods_path"],
                    )
                    ssn.merge(file)


def user_name():
    return pwd.getpwuid(os.getuid()).pw_name


def illumina_sql():
    return inspect.cleandoc(
        """
        SELECT study.id_study_lims AS study_id
          , sample.name AS sample_name
          , sample.supplier_name AS supplier_name
          , sample.public_name AS tol_specimen_id
          , sample.accession_number AS biosample_accession
          , sample.donor_id AS biospecimen_accession
          , sample.taxon_id AS taxon_id
          , 'Illumina' AS platform_type
          , run_lane_metrics.instrument_model AS instrument_model
          , flowcell.pipeline_id_lims AS pipeline_id_lims
          , CONVERT(run_lane_metrics.id_run, char) AS run_id
          , run_lane_metrics.run_complete AS run_complete
          , CONCAT(run_lane_metrics.id_run, '_', flowcell.position, '#', flowcell.tag_index) AS name_root
          , IF(product_metrics.qc IS NULL, NULL, IF(product_metrics.qc = 1, 'pass', 'fail')) AS lims_qc
          , run_lane_metrics.qc_complete AS qc_date
          , CONVERT(flowcell.tag_index, char) AS tag_index
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
        JOIN iseq_product_metrics AS product_metrics
          ON flowcell.id_iseq_flowcell_tmp = product_metrics.id_iseq_flowcell_tmp
        JOIN iseq_run_lane_metrics AS run_lane_metrics
          ON product_metrics.id_run = run_lane_metrics.id_run
          AND product_metrics.position = run_lane_metrics.position
        LEFT JOIN seq_product_irods_locations irods
          ON product_metrics.id_iseq_product = irods.id_product
        WHERE run_lane_metrics.qc_complete IS NOT NULL
          AND study.id_study_lims = %s
        """
    )


def pacbio_sql():
    return inspect.cleandoc(
        """
        SELECT study.id_study_lims AS study_id
          , sample.name AS sample_name
          , sample.supplier_name AS supplier_name
          , sample.public_name AS tol_specimen_id
          , sample.accession_number AS biosample_accession
          , sample.donor_id AS biospecimen_accession
          , sample.taxon_id AS taxon_id
          , 'PacBio' AS platform_type
          , well_metrics.instrument_type AS instrument_model
          , run.pipeline_id_lims AS pipeline_id_lims
          , well_metrics.movie_name AS run_id
          , well_metrics.run_complete AS run_complete
          , CONCAT(well_metrics.movie_name, "#", run.tag_identifier
              , IF(run.tag2_identifier IS NOT NULL
                  , CONCAT('#', run.tag2_identifier), '')) AS name_root
          , IF(well_metrics.qc_seq IS NULL, NULL
            , IF(well_metrics.qc_seq = 1, 'pass', 'fail')) AS lims_qc
          , well_metrics.qc_seq_date AS qc_date
          , run.well_label AS tag_index
          , run.tag_identifier AS tag1_id
          , run.tag2_identifier AS tag2_id
          , run.pac_bio_library_tube_name AS library_id
          , irods.irods_root_collection AS irods_path
          , irods.irods_data_relative_path AS irods_file
        FROM sample
        JOIN pac_bio_run AS run
          ON sample.id_sample_tmp = run.id_sample_tmp
        JOIN pac_bio_product_metrics AS product_metrics
          ON run.id_pac_bio_tmp = product_metrics.id_pac_bio_tmp
        JOIN pac_bio_run_well_metrics AS well_metrics
          ON product_metrics.id_pac_bio_rw_metrics_tmp = well_metrics.id_pac_bio_rw_metrics_tmp
        JOIN study
          ON run.id_study_tmp = study.id_study_tmp
        LEFT JOIN seq_product_irods_locations AS irods
          ON product_metrics.id_pac_bio_product = irods.id_product
        WHERE product_metrics.qc IS NOT NULL
          AND study.id_study_lims = %s
        """
    )


if __name__ == '__main__':
    main()
