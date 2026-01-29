import inspect
import logging
import os
import re
import sys
from functools import cache
from io import StringIO
from pathlib import Path
from tempfile import NamedTemporaryFile

import click
from tol.core import DataSourceFilter

from tola import click_options, db_connection, tolqc_client
from tola.diff_mlwh.database import MLWHDiffDB
from tola.diff_mlwh.diff_store import pretty_diff_iterator
from tola.goat_client import GoaTClient
from tola.ndjson import ndjson_row
from tola.tqc.sts import fetch_specimen_info_for_specimens, update_specimen_fields

log = logging.getLogger(__name__)


@click.command()
@click_options.tolqc_url
@click_options.api_token
@click_options.tolqc_alias
@click_options.log_level
@click_options.write_to_stdout
@click.option(
    "--diff-mlwh/--no-diff-mlwh",
    "run_diff_mlwh",
    default=False,
    show_default=True,
    help="Run diff-mlwh on the same data fetched from MLWH after import to ToLQC",
)
@click_options.diff_mlwh_duckdb
@click.argument(
    "study_id_list",
    type=click.INT,
    nargs=-1,
    required=False,
)
def cli(
    tolqc_url,
    api_token,
    tolqc_alias,
    log_level,
    study_id_list,
    write_to_stdout,
    run_diff_mlwh,
    diff_mlwh_duckdb,
):
    """
    Fetch sequencing data from the Multi-LIMS Warehouse (MLWH)

    Fetches both Illumina and PacBio sequencing run data by querying the MLWH
    MySQL database, and prints out a report of new and changed data.

    Where each STUDY_ID is a numeric ID, e.g. 5901 (Darwin Tree of Life).

    Iterates over each study in the ToLQC database if no STUDY_IDs are
    supplied.
    """

    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(message)s",
        force=True,
    )

    client = tolqc_client.TolClient(tolqc_url, api_token, tolqc_alias)
    if not study_id_list:
        study_id_list = client.list_auto_sync_study_ids()
    mlwh = db_connection.mlwh_db()

    if write_to_stdout:
        write_mlwh_data_to_filehandle(mlwh, study_id_list, sys.stdout)
    else:
        mlwh_data = (
            NamedTemporaryFile("w", prefix="mlwh_", suffix=".ndjson")  # noqa: SIM115
            if run_diff_mlwh
            else None
        )

        new_specimens = {}
        for study_id in study_id_list:
            for platform, run_data_fetcher in (
                ("PacBio", pacbio_fetcher),
                ("Illumina", illumina_fetcher),
            ):
                row_itr = run_data_fetcher(mlwh, study_id, mlwh_data)
                rspns = client.ndjson_post("loader/seq-data", row_itr)
                click.echo(formatted_response(rspns, study_id, platform), nl=False)
                record_new_specimens(new_specimens, rspns)

        # Import any missing specimen info from STS for newly added specimens
        if specimen_names := list(new_specimens):
            patches = fetch_specimen_info_for_specimens(client, specimen_names)
            if upsrtr := update_specimen_fields(
                client, specimen_names, patches, apply_flag=True
            ):
                click.echo("")
                click.echo(upsrtr.page_results(apply_flag=True, plain_text=True))

        if run_diff_mlwh:
            # Ensure data is flushed to storage
            mlwh_data.flush()
            os.fsync(mlwh_data.fileno())

            diff_db = MLWHDiffDB(diff_mlwh_duckdb, write_flag=True)
            diff_db.update(client, Path(mlwh_data.name))
            diffs = diff_db.fetch_stored_diffs(show_new_diffs=True)
            for out in pretty_diff_iterator(diffs):
                click.echo(out)

        patch_species(client)
        patch_specimens(client)


def write_mlwh_data_to_filehandle(mlwh, study_id_list, fh):
    for study_id in study_id_list:
        for run_data_fetcher in (pacbio_fetcher, illumina_fetcher):
            for row in run_data_fetcher(mlwh, study_id):
                fh.write(row)


def fetch_mlwh_seq_data_to_file(tqc, mlwh_ndjson):
    mlwh = db_connection.mlwh_db()
    write_mlwh_data_to_filehandle(
        mlwh, tqc.list_auto_sync_study_ids(), mlwh_ndjson.open("w")
    )


def record_new_specimens(new_specimens, response):
    if new := response.get("new"):
        for row in new:
            if spcmn := row.get("specimen"):
                new_specimens[spcmn] = True


def formatted_response(response, study_id, platform):
    out = StringIO("")

    if new := response.get("new"):
        out.write(f"\n\nNew {platform} data in '{new[0]['study']} ({study_id})':\n\n")
        for row in new:
            out.write(response_row_std_fields(row))

    if upd := response.get("updated"):
        out.write(
            f"\n\nUpdated {platform} data in '{upd[0]['study']} ({study_id})':\n\n"
        )
        for row in upd:
            out.write(response_row_std_fields(row))
            for col, old_new in row["changes"].items():
                out.write(f"  {col} changed from {old_new[0]!r} to {old_new[1]!r}\n")

    return out.getvalue()


def response_row_std_fields(row):
    return "\t".join(str(row[x]) for x in row if x not in ("study", "changes")) + "\n"


def illumina_fetcher(mlwh, study_id, save_data=None):
    log.info(f"Fetching Illumina data for study '{study_id}'")
    crsr = mlwh.cursor(dictionary=True)
    crsr.execute(illumina_sql(), [str(study_id)])
    for row in crsr:
        build_remote_path(row)
        fmt = ndjson_row(row)
        if save_data:
            save_data.write(fmt)
        yield fmt


PIPELINE_TO_LIBRARY_TYPE = {
    "PacBio_Ultra_Low_Input": "PacBio - HiFi (ULI)",
    "PacBio_Ultra_Low_Input_mplx": "PacBio - HiFi (Ampli-Fi)",
    "Pacbio_HiFi": "PacBio - HiFi",
    "Pacbio_HiFi_mplx": "PacBio - HiFi",
    "Pacbio_IsoSeq": "PacBio - IsoSeq",
    "PacBio_IsoSeq_mplx": "PacBio - IsoSeq",
    "Pacbio_Microbial_mplx": "PacBio - HiFi (Microbial)",
}


def pacbio_fetcher(mlwh, study_id, save_data=None):
    log.info(f"Fetching PacBio data for study '{study_id}'")
    crsr = mlwh.cursor(dictionary=True)
    crsr.execute(pacbio_sql(), [str(study_id)])
    for row in crsr:
        build_remote_path(row)
        extract_pimms_description(row)

        # Build data_id field, appending any tags
        data_id = row["data_id"]
        tag1 = trimmed_tag(row["tag1_id"])
        tag2 = trimmed_tag(row["tag2_id"])
        if tag2:
            row["data_id"] = f"{data_id}#{tag1}#{tag2}"
        elif tag1:
            row["data_id"] = f"{data_id}#{tag1}"

        # Map MLWH library type to canonical library type
        if pidl := row.get("pipeline_id_lims"):
            row["pipeline_id_lims"] = PIPELINE_TO_LIBRARY_TYPE.get(pidl, pidl)
        fmt = ndjson_row(row)
        if save_data:
            save_data.write(fmt)
        yield fmt


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


def build_remote_path(row):
    irods_path = row.pop("irods_path")
    irods_file = row.pop("irods_file")
    if irods_path and irods_file:
        row["remote_path"] = "irods:" + irods_path.rstrip("/") + "/" + irods_file
    else:
        row["remote_path"] = None


def extract_pimms_description(row):
    for col in "sample_type", "sample_description":
        if (txt := row.pop(col)) and re.search(r"\bPiMmS\b", txt, re.IGNORECASE):
            row["pipeline_id_lims"] = "PacBio - HiFi (PiMmS)"
            row["pcr_adapter_id"] = "TruSeq_i7s_UDI001--TruSeq_i5s_UDI001"


@cache
def illumina_sql():
    """
    The STRAIGHT_JOIN forces the optimizer to join the tables in the order in
    which they are listed in the FROM clause.
    """

    return inspect.cleandoc(
        """
        SELECT STRAIGHT_JOIN
            REGEXP_REPLACE(
              -- Trim file suffix, i.e. ".cram"
              irods.irods_data_relative_path
                , '\\.[[:alnum:]]+$'
                , ''
            ) AS data_id
          , CONVERT(study.id_study_lims, SIGNED) AS study_id
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
          , COALESCE(
              REGEXP_SUBSTR(
                -- From e.g. "48906_5-6#1.cram" extract "5-6"
                irods.irods_data_relative_path
                , '(?<=_)[^_]+(?=#)'
              )
              -- Fallback if pattern match fails
              , run_lane_metrics.position
            ) AS element
          , flowcell.pipeline_id_lims AS pipeline_id_lims
          , COALESCE(
              REGEXP_SUBSTR(
                -- From e.g. "48906_5-6#1.cram" extract "48906_5-6"
                irods.irods_data_relative_path
                , '^[^#\\.]+'
              )
              -- Fallback if pattern match fails
              , CONVERT(product_metrics.id_run, CHAR)
            ) AS run_id
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
        SELECT STRAIGHT_JOIN
            well_metrics.movie_name AS data_id
          , CONVERT(study.id_study_lims, SIGNED) AS study_id
          , sample.name AS sample_name
          , sample.description AS sample_description
          , sample.sample_type AS sample_type
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
          , IF(well_metrics.plate_number IS NULL
              , well_metrics.well_label
              , CONCAT(well_metrics.well_label
                  , '.'
                  , well_metrics.plate_number)
                ) AS element
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
          , well_metrics.movie_minutes
          , well_metrics.binding_kit
          , well_metrics.sequencing_kit
          , well_metrics.sequencing_kit_lot_number
          , well_metrics.cell_lot_number
          , IF(well_metrics.include_kinetics IS NULL, NULL,
              IF(well_metrics.include_kinetics = 1, 'true', 'false')
            ) AS include_kinetics
          , well_metrics.loading_conc
          , well_metrics.control_num_reads
          , well_metrics.control_read_length_mean
          , well_metrics.control_concordance_mean
          , well_metrics.control_concordance_mode
          , well_metrics.local_base_rate
          , well_metrics.polymerase_read_bases
          , well_metrics.polymerase_num_reads
          , well_metrics.polymerase_read_length_mean
          , well_metrics.polymerase_read_length_n50
          , well_metrics.insert_length_mean
          , well_metrics.insert_length_n50
          , well_metrics.unique_molecular_bases
          , well_metrics.productive_zmws_num
          , well_metrics.p0_num
          , well_metrics.p1_num
          , well_metrics.p2_num
          , well_metrics.adapter_dimer_percent
          , well_metrics.short_insert_percent
          , well_metrics.hifi_read_bases
          , well_metrics.hifi_num_reads
          , well_metrics.hifi_read_length_mean
          , well_metrics.hifi_read_quality_median
          , well_metrics.hifi_number_passes_mean
          , well_metrics.hifi_low_quality_read_bases
          , well_metrics.hifi_low_quality_num_reads
          , well_metrics.hifi_low_quality_read_length_mean
          , well_metrics.hifi_low_quality_read_quality_median
          , well_metrics.hifi_barcoded_reads
          , well_metrics.hifi_bases_in_barcoded_reads

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
        WHERE irods.irods_data_relative_path IS NOT NULL
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
            "tolid_prefix": None,
            "taxon_family": None,
            "taxon_order": None,
            "taxon_phylum": None,
            "taxon_group": None,
        }
    )
    gc = GoaTClient()
    updates = []
    for sp in ads.get_list("species", object_filters=filt):
        if spec_info := gc.get_species_info(sp.taxon_id):
            if sp.id != spec_info["species_id"]:
                if not reassign_species(client, sp, spec_info):
                    log.info(
                        f"Species with taxon_id = '{sp.taxon_id}' should be"
                        f" named '{spec_info['species_id']}' not '{sp.id}'"
                    )
                continue

            # Do not overwrite exisiting attributes but fill in any blanks
            changes = {}
            for prop, val in spec_info.items():
                if prop == "species_id":
                    continue
                if getattr(sp, prop) is None:
                    changes[prop] = val
            if changes:
                updates.append(obj_factory("species", id_=sp.id, attributes=changes))
                log.debug(f"Updating Species '{sp.id}' fields: {changes}")
    for page in client.pages(updates):
        ads.upsert("species", page)


def patch_specimens(client):
    ads = client.ads
    obj_factory = ads.data_object_factory
    filt = DataSourceFilter(
        exact={
            "ploidy": None,
        }
    )
    gc = GoaTClient()
    updates = []
    for spmn in ads.get_list(
        "specimen",
        object_filters=filt,
        requested_fields=["species"],
    ):
        if not spmn.species or spmn.species.id == "unidentified":
            continue
        taxon_id = spmn.species.taxon_id
        goat_info = gc.get_species_info(taxon_id) if taxon_id else None
        ploidy = specimen_ploidy(spmn, goat_info) or 2
        updates.append(
            obj_factory("specimen", id_=spmn.id, attributes={"ploidy": ploidy})
        )
    for page in client.pages(updates):
        ads.upsert("specimen", page)


def specimen_ploidy(specimen, goat_info):
    """
    Set haploid for all male hymenoptera.

    Set haploid for all mosses, hornworts and green algae unless a `direct`
    estimate of ploidy is returned for that species by GoaT.
    """

    tol_id = specimen.id
    prefix = tol_id[0:2]
    sex = specimen.sex_id
    if prefix == "iy" and sex == "Male":
        # Male hymenoptera
        return 1
    elif prefix in {
        "ca",  # Mosses / Bryophyta (Andreaeopsida only)
        "cs",  # Mosses / Bryophyta (Sphagnopsida only)
        "cb",  # Mosses / Bryophyta
        "cn",  # Hornworts / Anthocerotophyta
        "uk",  # Green algae / Charophyta
    }:
        if goat_info and "direct" in goat_info["ploidy_sources"]:
            return goat_info["ploidy"]
        return 1

    return goat_info["ploidy"] if goat_info else None


def reassign_species(client, bad_sp, spec_info):
    ads = client.ads
    obj_factory = ads.data_object_factory

    # Is there an existing species with the correct name?
    (good_sp,) = ads.get_by_ids("species", [spec_info["species_id"]])
    if not good_sp:
        return

    # Move specimens from the bad to the good species
    updates = []
    for spcmn in bad_sp.specimens:
        updates.append(
            obj_factory(
                "specimen",
                id_=spcmn.id,
                attributes={
                    "species_id": good_sp.id,
                },
            )
        )
    if updates:
        ads.upsert("specimen", updates)

    # Delete any edits of the bad species
    if edits := bad_sp.edit_history:
        ads.delete("edit_species", [x.id for x in edits])

    # Delete the bad species entry
    ads.delete("species", [bad_sp.id])

    return good_sp


if __name__ == "__main__":
    cli()
