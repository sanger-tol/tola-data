import logging
import re
import sys

import click
from partisan.irods import AVU, Collection, query_metadata

from tola import click_options, tolqc_client
from tola.ndjson import ndjson_row


@click.command()
@click_options.tolqc_url
@click_options.api_token
@click_options.tolqc_alias
@click_options.log_level
@click_options.write_to_stdout
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
):
    """
    Fetch sequencing data from the Multi-LIMS Warehouse (MLWH)

    Fetches Oxford Nanopore (ONT) sequencing run data by querying iRODS
    under "/seq/ont" for data linked to any of numeric STUDY_ID procided. e.g. 5901
    (Darwin Tree of Life).

    If STUDY_ID arguments are not provided, a list is fetched from the ToLQC
    database where "study.auto_sync = true".
    """

    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(message)s",
        force=True,
    )

    client = tolqc_client.TolClient(tolqc_url, api_token, tolqc_alias)
    if not study_id_list:
        study_id_list = client.list_auto_sync_study_ids()

    ont_rows = []
    for study_id in study_id_list:
        ont_rows.extend(fetch_ont_irods_data_for_study(study_id))
    if write_to_stdout or True:
        for row in ont_rows:
            sys.stdout.write(ndjson_row(row))


def fetch_ont_irods_data_for_study(study_id):
    study_data = []
    for coll in query_metadata(
        AVU("study_id", study_id),
        collection=True,
        data_object=False,
        zone="seq",
    ):
        coll_path = str(coll.path)

        # No way to restrict iRODS metadata query to path prefix ⁉️
        # (It is a bug in partisan, which is being fixed.)
        if not coll_path.startswith("/seq/ont/") or re.search(
            # Skip lines containg the word "fail"
            r"(\b|_)fail(\b|_)",
            coll_path,
            re.IGNORECASE,
        ):
            continue

        product_dir, file_type = product_type_from_sub_collections(coll)

        avu_dict = {}
        for avu in coll.metadata(ancestors=True):  ### timestamps=True
            avu_dict[avu.attribute.replace(":", "_")] = avu.value

        run_id = build_run_id(avu_dict)
        data_id = build_data_id(run_id, avu_dict)


        row = {
            "data_id": data_id,
            "study_id": study_id,
            "sample_name": avu_dict.get("sample_name"),
            "supplier_name": avu_dict.get("sample_supplier_name"),
            "biosample_accession": avu_dict.get("sample_accession_number"),
            "biospecimen_accession": avu_dict.get("sample_donor_id"),
            "scientific_name": avu_dict.get("sample_common_name"),
            # taxon_id
            "platform_type": "ONT",
            "instrument_model": avu_dict.get("ont_device_type"),
            "instrument_name": avu_dict.get("ont_hostname"),
            "element": build_element(avu_dict),
            "run_id": run_id,
            "run_complete": None,
            "tag1_id": avu_dict.get("ont_tag_identifier"),
            "remote_path": f"irods:{coll_path}",
            "file_type": file_type,
            "product_dir": product_dir,
            # **avu_dict,
        }
        logging.debug(f"{row = }")
        study_data.append(row)

    return study_data


def product_type_from_sub_collections(coll):
    """
    ┌───────────────┬──────────────┐
    │     type      │ count_star() │
    │    varchar    │    int64     │
    ├───────────────┼──────────────┤
    │ bam_fail      │           33 │
    │ bam_pass      │           33 │
    │ fail          │           40 │
    │ fastq_fail    │            8 │
    │ fastq_pass    │            8 │
    │ nextflow      │           40 │
    │ other_reports │           41 │
    │ pass          │           40 │
    │ pod5          │           41 │
    │ pod5_skip     │           12 │
    │ qc            │           40 │
    ├───────────────┴──────────────┤
    │ 11 rows            2 columns │
    └──────────────────────────────┘
    """
    sub_coll = sub_collection_names(coll)
    product_dir = None
    file_type = None
    for dir_name, type_name in (
        ("pass", "FASTQ_DIR"),
        ("bam_pass", "RAW_BAM_DIR"),
        ("fastq_pass", "RAW_FASTQ_DIR"),
        ("pod5", "RAW_POD5_DIR"),
    ):
        if dir_name in sub_coll:
            product_dir = dir_name
            file_type = type_name
            break

    return product_dir, file_type


def sub_collection_names(coll):
    sub_names = set()
    for c in coll.contents():
        if not isinstance(c, Collection):
            continue
        sub_names.add(c.path.name)

    return sub_names


def build_run_id(avu_dict):
    # Avoid using run IDs which are just an integer
    run_id = avu_dict.get("ont_experiment_name")
    if re.fullmatch(r"^\d+$", run_id):
        run_id = avu_dict.get("ont_hostname", "ONT-X") + "-" + run_id

    return run_id


def build_data_id(run_id, avu_dict):
    data_id_components = [run_id]
    for field in (
        "ont_flowcell_id",
        "ont_instrument_slot",
        "ont_tag_identifier",
    ):
        if cmpt := avu_dict.get(field):
            data_id_components.append(cmpt)

    return "#".join(data_id_components) if data_id_components else None


def build_element(avu_dict):
    element_components = []
    for field in (
        "ont_instrument_slot",
        "ont_tag_identifier",
    ):
        if cmpt := avu_dict.get(field):
            element_components.append(cmpt)

    return ".".join(element_components) if element_components else None
