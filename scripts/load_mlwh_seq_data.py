import datetime
import inspect
import os
import pwd
import sys

from sqlalchemy import select
from tol.api_client import ApiDataSource, ApiObject
from tol.core import DataSourceFilter
from main.model import (
    Allocation,
    Base,
    Data,
    File,
    Project,
    Run,
    Sample,
    Species,
    Specimen,
    User,
)
from . import db_connection


def main(opt=""):
    mlwh = db_connection.mlwh_db()
    sts = db_connection.sts_db()
    if "api" in opt.lower():
        load_via_tol_api(mlwh, sts)
    else:
        load_via_sqlalchemy(mlwh, sts)


def load_via_tol_api(mlwh, sts):
    tolqc = ApiDataSource(
        {
            'url': os.getenv('TOLQC_URL') + '/api/v1',
            'key': os.getenv('TOLQC_API_KEY'),
        }
    )
    project_ids = tuple(
        p.lims_id for p in tolqc.get_list('projects') if p.lims_id is not None
    )
    for proj_lims_id in project_ids:
        for get_sql in illumina_sql, pacbio_sql:
            ### Iterate through projects ###
            crsr = mlwh.cursor(dictionary=True)
            crsr.execute(get_sql(), (proj_lims_id,))


class TolSqlMarshall:
    def __init__(self, session=None, mlwh=None, sts=None):
        self.mlwh = mlwh
        self.sts = sts
        self.session = session
        self.user_id = effective_user_id(session)

    def fetch_or_create(self, cls, spec, selector=None):
        # Use the primary key if there is no selector
        if not selector:
            if hasattr(cls.Meta, 'id_column'):
                selector = (cls.Meta.id_column,)
            else:
                selector = ('id',)

        # Build a filter for the select query
        fltr = {}
        for sel in selector:
            sel_val = spec.get(sel)
            if sel_val is not None:
                fltr[sel] = sel_val
        if len(fltr) != len(selector):
            raise Exception(f"For selector={selector} passed spec is missing fields")

        query = select(cls).filter_by(**fltr)
        ssn = self.session
        if db_obj := ssn.scalars(query).one_or_none():
            # Object matching selector fields is already in database
            return db_obj
        else:
            if cls.has_log_details():
                self.add_log_fields(spec)
            obj = cls(**spec)
            ssn.add(obj)
            ssn.flush()
            return obj

    def add_log_fields(self, spec):
        now = datetime.datetime.now(datetime.timezone.utc)
        if not spec.get("created_at"):
            spec["last_modified_at"] = now
        if not spec.get("created_by"):
            spec["created_by"] = self.user_id
        spec["last_modified_at"] = now
        spec["last_modified_by"] = self.user_id


def load_via_sqlalchemy(mlwh, sts):
    engine, Session = db_connection.local_postgres_engine(echo=True)

    with Session() as ssn:
        mrshl = TolSqlMarshall(session=ssn, mlwh=mlwh, sts=sts)
        for get_sql in illumina_sql, pacbio_sql:
            ### Iterate through projects ###
            crsr = mlwh.cursor(dictionary=True)
            crsr.execute(get_sql(), (5901,))
            for row in crsr:
                if rec_count == 0:
                    continue
                data = mrshl.fetch_or_create(
                    Data,
                    {
                        "name_root": row["name_root"],
                        "tag_index": row["tag_index"],
                        "tag1_id": row["tag1_id"],
                        "tag2_id": row["tag2_id"],
                        "lims_qc": row["lims_qc"],
                        "date": row["qc_date"],
                    },
                    ('name_root',),
                )
                if row["irods_path"]:
                    file = mrshl.fetch_or_create(
                        File,
                        {
                            "data_id": data.data_id,
                            "name": row["irods_file"],
                            "remote_path": row["irods_path"],
                        },
                        ('data_id',),
                    )

        ssn.commit()


def effective_user_id(ssn):
    user_name = pwd.getpwuid(os.getuid()).pw_name
    query = select(User).filter_by(email=f"{user_name}@sanger.ac.uk")
    user = ssn.scalars(query).unique().one()
    return user.id


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
    main(*sys.argv[1:])
