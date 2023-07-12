import inspect
import logging
import re
import sys
import tola.marshals

from main.model import (
    Allocation,
    Data,
    File,
    Run,
    Sample,
    Species,
    Specimen,
)
from tola import db_connection

logging.basicConfig(level=logging.INFO)


def main():
    mrshl, _ = tola.marshals.marshal_from_command_line(
        "Import sequencing run data from the MLWH"
    )
    mlwh = db_connection.mlwh_db()
    sts = db_connection.sts_db()
    load_mlwh_data(mrshl, mlwh, sts)
    mrshl.commit()


def isoformat_if_date(dt):
    return dt.isoformat() if hasattr(dt, "isoformat") else dt


def load_mlwh_data(mrshl, mlwh, sts):
    sci_taxon = mrshl.fetch_sci_taxon_dict()
    species_fetcher = sts_species_fetcher(sts)
    for mlwh_sql in illumina_sql(), pacbio_sql():
        # Iterate through projects
        for project in mrshl.list_projects():
            crsr = mlwh.cursor(dictionary=True)
            crsr.execute(mlwh_sql, (project.lims_id,))
            for row in crsr:
                try:
                    store_row_data(mrshl, row, species_fetcher, project, sci_taxon)
                except Exception as err:
                    msg = f"Error saving row:\n{format_row(row)}\n{err}"
                    raise Exception(msg) from err


def store_row_data(mrshl, row, species_fetcher, project, sci_taxon):
    # Species
    species_info = species_fetcher(row["taxon_id"], row["scientific_name"])
    if not species_info:
        row_info(row, "Missing species info")
        return
    elif stored_tax_id := sci_taxon.get(species_info["species_id"]):
        if stored_tax_id != species_info["taxon_id"]:
            row_info(
                row, f"Have different taxon_id={stored_tax_id} for scientific_name"
            )
            return
    else:
        sci_taxon[species_info["species_id"]] = species_info["taxon_id"]

    species = mrshl.fetch_or_create(Species, species_info, ("taxon_id",))

    if not row["tol_specimen_id"]:
        row_info(row, "Missing tol_specimen_id")
        return

    # specimen Accession
    # Specimen
    specimen = mrshl.fetch_or_create(
        Specimen,
        {
            "specimen_id": row["tol_specimen_id"],
            "species_id": species.species_id,
            # "hierarchy_name": row["tol_specimen_id"],
            # "accession_id": row["biospecimen_accession"],
        },
    )

    # sample Accession
    # Sample
    sample = mrshl.fetch_or_create(
        Sample,
        {
            "sample_id": row["sample_name"],
            "specimen_id": row["tol_specimen_id"],
            # "accession_id": row["biosample_accession"],
        },
    )

    # Data
    data = mrshl.fetch_or_create(
        Data,
        {
            "name_root": row["name_root"],
            "sample_id": row["sample_name"],
            "tag_index": row["tag_index"],
            "tag1_id": row["tag1_id"],
            "tag2_id": row["tag2_id"],
            "lims_qc": row["lims_qc"],
            "date": isoformat_if_date(row["qc_date"]),
        },
        ("name_root",),
    )

    # Allocation
    allocation = mrshl.fetch_or_create(
        Allocation,
        {
            "project_id": project.project_id,
            "data_id": data.data_id,
        },
        ("project_id", "data_id"),
    )

    # File
    if row["irods_path"]:
        file = mrshl.fetch_or_create(
            File,
            {
                "data_id": data.data_id,
                "name": row["irods_file"],
                "remote_path": row["irods_path"],
            },
            ("data_id",),
        )


def sts_species_fetcher(sts):
    sql = inspect.cleandoc(
        """
        SELECT scientific_name
          , common_name
          , family
          , order_group
          , genome_size
        FROM species
        WHERE taxonid = %s
        """
    )
    crsr = sts.cursor()

    def fetcher(taxon_id, mlwh_sci_name):
        crsr.execute(sql, (str(taxon_id),))
        if crsr.rowcount == 0:
            return (
                minimal_species_info(taxon_id, mlwh_sci_name) if mlwh_sci_name else None
            )
        elif crsr.rowcount != 1:
            raise Exception(
                f"Expecting one row for taxon_id='{taxon_id}' got '{crsr.rowcount}' rows"
            )
        row = crsr.fetchone()
        sci_name = row["scientific_name"]
        hierarchy_name = re.sub(r"\s+", "_", sci_name)
        return {
            "species_id": sci_name,
            "hierarchy_name": hierarchy_name,
            "common_name": row["common_name"],
            "taxon_id": taxon_id,
            "taxon_family": row["family"],
            "taxon_order": row["order_group"],
            # "genome_size": row["genome_size"],  # BigInteger required
        }

    return fetcher


def minimal_species_info(taxon_id, sci_name):
    hierarchy_name = re.sub(r"\s+", "_", sci_name)
    return {
        "species_id": sci_name,
        "taxon_id": taxon_id,
        "hierarchy_name": hierarchy_name,
    }


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
          AND sample.taxon_id IS NOT NULL
          AND sample.public_name IS NOT NULL
          AND study.id_study_lims = %s
        HAVING name_root IS NOT NULL
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
          , sample.common_name AS scientific_name
          , sample.taxon_id AS taxon_id
          , 'PacBio' AS platform_type
          , well_metrics.instrument_type AS instrument_model
          , run.pipeline_id_lims AS pipeline_id_lims
          , well_metrics.pac_bio_run_name AS run_id
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
          AND sample.taxon_id IS NOT NULL
          AND study.id_study_lims = %s
        HAVING name_root IS NOT NULL
        """
    )


def row_info(row, msg):
    print(f"{msg}:\n{format_row(row)}", file=sys.stderr)


def format_row(row):
    name_max = max(len(name) for name in row)
    return "".join(
        f"  {name:>{name_max}} = {'' if row[name] is None else row[name]}\n"
        for name in row
    )


if __name__ == '__main__':
    main()
