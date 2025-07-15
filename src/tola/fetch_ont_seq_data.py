import inspect
import logging
import pathlib
import re
import sys
from datetime import datetime, timedelta

import click
import pytz
from partisan.irods import AVU, Collection, Timestamp, query_metadata

from tola import click_options, db_connection, tolqc_client
from tola.fetch_mlwh_seq_data import response_row_std_fields
from tola.ndjson import ndjson_row, parse_ndjson_stream
from tola.tqc.upsert import TableUpserter


class MetadataMismatchError(ValueError):
    """
    iRODS metadata of two rows does not match
    """


"""
Flowcells do get re-used between runs e.g. when a run is stopped for
operational reasons and restarted in the same slot on the instrument, or
maybe is restarted in a different slot (perhaps the first slot had a
temperature stability problem). To find things in iRODS we use a tuple of
ont:experiment_name, ont:flowcell_id and ont:instrument_slot (which are
experiment_name, flowcell_id and instrument_slot in oseq_flowcell
"""


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
@click.option(
    "--since",
    "since",
    help="""
      Show data modified since a particular datetime, in ISO8601
      (RFC3339) format. If there is no time zone it is assumed to be UTC /
      GMT.
    """,
)
@click.option(
    "--input-file",
    "input_files",
    type=click.Path(
        path_type=pathlib.Path,
        readable=True,
        dir_okay=False,
        allow_dash=True,
    ),
    multiple=True,
    help="One or more ND-JSON input files in the format produced by '--stdout'",
)
def cli(
    tolqc_url,
    api_token,
    tolqc_alias,
    log_level,
    study_id_list,
    write_to_stdout,
    since,
    input_files,
):
    """
    Fetch sequencing data from the Multi-LIMS Warehouse (MLWH)

    Fetches Oxford Nanopore (ONT) sequencing run data by querying iRODS
    under "/seq/ont" for data linked to any of numeric STUDY_ID procided. e.g. 5901
    (Darwin Tree of Life).

    If STUDY_ID arguments are not provided, a list is fetched from the ToLQC
    database where "study.auto_sync = true".
    """

    client = tolqc_client.TolClient(tolqc_url, api_token, tolqc_alias, page_size=25)
    if since:
        since = utc_datetime(since)

    if not since and not write_to_stdout:
        since = get_irods_ont_last_modified(client)
        if since:
            # Subtract a slop factor from the last modification time to ensure
            # that any overlapping, potentially out of order, runs are
            # detected.
            since -= timedelta(days=40)
    since_query = (
        [
            # These are the defaults for event and operator, but are specified
            # for clarity:
            Timestamp(
                since,
                event=Timestamp.Event.MODIFIED,
                operator="n>=",
            )
        ]
        if since
        else None
    )

    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(message)s",
        force=True,
    )

    if not study_id_list:
        study_id_list = client.list_auto_sync_study_ids()

    ont_rows = []
    if input_files:
        for file in input_files:
            fh = click.open_file(file)
            ont_rows.extend(parse_ndjson_stream(fh))
    else:
        for study_id in study_id_list:
            ont_rows.extend(fetch_ont_irods_data_for_study(study_id, since_query))
        add_mlwh_sample_data(ont_rows)

    if write_to_stdout:
        for row in ont_rows:
            sys.stdout.write(ndjson_row(row))
    else:
        store_ont_data(client, ont_rows)


def fetch_ont_irods_data_for_study(study_id, since_query):
    study_data = []
    for coll in query_metadata(
        AVU("study_id", study_id),
        timestamps=since_query,
        collection=True,
        data_object=False,
        zone="/seq/ont/",  # Restricts query to Oxford Nanopore data
    ):
        coll_path = str(coll.path)

        # Skip collections (directories) containing the word "fail"
        if re.search(
            r"(\b|_)fail(\b|_)",
            coll_path,
            re.IGNORECASE,
        ):
            continue

        product_dirs = product_from_collection(coll)

        avu_dict = {}
        for avu in coll.metadata(
            ancestors=True,  # Mutiplexed data needs metadata from further up the tree
        ):
            # Remove any "ont:" prefix so that the field names match the names
            # in the mlwh.oseq_flowcell table.
            avu_dict[avu.attribute.replace("ont:", "")] = avu.value

        run_id = build_run_id(avu_dict)
        data_id = build_data_id(avu_dict, run_id)

        # fmt: off
        row = {
            "data_id":                data_id,
            "study_id":               study_id,
            "sample_name":            avu_dict.get("sample"),
            "supplier_name":          avu_dict.get("sample_supplier_name"),
            "tol_specimen_id":        avu_dict.get("sample_public_name"),
            "biosample_accession":    avu_dict.get("sample_accession_number"),
            "biospecimen_accession":  avu_dict.get("sample_donor_id"),
            "scientific_name":        avu_dict.get("sample_common_name"),
            "platform_type":          "ONT",
            "instrument_model":       avu_dict.get("device_type"),
            "instrument_name":        avu_dict.get("hostname"),
            "run_id":                 run_id,
            "run_start":              coll.created(),
            # Not actually a QC date.  Destined for data.date:
            "qc_date":                coll.modified(),
            "tag1_id":                avu_dict.get("tag_identifier"),
            "tag2_id":                avu_dict.get("tag2_identifier"),
            "collection":             coll_path,
            "product_dirs":           product_dirs,
        }
        # fmt: on

        logging.debug(f"{row = }")
        study_data.append(row)

    return merge_by_data_id(study_data)


def utc_datetime(txt):
    dt = datetime.fromisoformat(txt)
    if not dt.tzinfo or dt.tzinfo.utcoffset(dt) is None:
        return pytz.timezone("UTC").localize(dt)
    else:
        return dt


def get_irods_ont_last_modified(client):
    (upd,) = client.ads.get_by_ids("metadata", ["irods.ont.last_modified"])
    return upd.timestamp_value if upd else None


def store_irods_ont_last_modified(client, timestamp):
    tbl = "metadata"
    client.ads.upsert(
        tbl,
        [
            client.build_cdo(
                tbl,
                "irods.ont.last_modified",
                {
                    "description": (
                        "The most recent modified time of all iRODS Collections"
                        ' returned by the metadata query of "/seq/ont/"'
                    ),
                    "timestamp_value": timestamp,
                },
            )
        ],
    )


def update_stored_max_modified(client, max_modified):
    if isinstance(max_modified, str):
        max_modified = datetime.fromisoformat(max_modified)
    stored_max = get_irods_ont_last_modified(client)
    if not stored_max or max_modified > stored_max:
        store_irods_ont_last_modified(client, max_modified)


def merge_by_data_id(study_data):
    by_data_id = {}
    skip_fields = {"collection", "product_dirs"}
    for row in study_data:
        data_id = row["data_id"]
        if xst := by_data_id.get(data_id):
            # Merge into the existing record
            merge_row_check_matches(data_id, skip_fields, xst, row)
        else:
            # Create a new record, which will have any other rows with the
            # same data_id merged into it.
            xst = {k: v for k, v in row.items() if k not in skip_fields}
            xst["files"] = row["product_dirs"]
            by_data_id[data_id] = xst

    return list(by_data_id.values())


def merge_row_check_matches(data_id, skip_fields, xst, row):
    """
    Merges two rows of data, which will usually represent the raw data off the
    machine an the re-basecalled "offline" data. Checks that all fields,
    apart from the exceptions, match.

    The "product_dirs" field is the location of ONT data on iRODS, so we
    extend the "files" list.

    For the datetime fields, the earliest "run_start" (created) will be the
    completion time of the sequencing run, and the latest "qc_date"
    (modified) will be when the re-basecalling finished.
    """
    for k, v in row.items():
        if k == "product_dirs":
            xst["files"].extend(v)
        elif k in skip_fields:
            continue
        elif v != (xst_v := xst.get(k)):
            if xst_v is None:
                xst[k] = v
            elif v is not None:
                if k == "run_start":
                    # Take the earliest run_start datetime
                    if v < xst_v:
                        xst[k] = v
                elif k == "qc_date":
                    # Take the latest qc_date datetime
                    if v > xst_v:
                        xst[k] = v
                else:
                    msg = (
                        f"Mismatched values for {data_id = }, {k!r}:"
                        f" {xst.get(k)!r} != {v!r}"
                    )
                    raise MetadataMismatchError(msg)


def product_from_collection(coll):
    """
    Subdirectories of collections found in ToL ONT data:

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
    top_types, sub_names = sub_collection_names(coll)

    type_prefix = "RECALL" if "/offline-" in str(coll.path) else "RAW"
    product_list = []

    if "FASTQ" in top_types:
        append_product_dir(
            product_list,
            coll.path,
            type_prefix,
            "FASTQ_DIR",
        )

    # fmt: off
    for dir_name, dir_type in (
        ("reads",       "FAST5_TAR_DIR"),
        ("fast5_pass",  "FAST5_DIR"),
        ("pod5",        "POD5_DIR"),
        ("fastq_pass",  "FASTQ_DIR"),
        ("pass",        "FASTQ_DIR"),
        ("pass_split",  "FASTQ_DIR"),
        ("bam_pass",    "BAM_DIR"),
    ):
        if dir_name in sub_names:
            append_product_dir(
                product_list,
                coll.path / dir_name,
                type_prefix,
                dir_type,
            )
    # fmt: on

    return product_list


def append_product_dir(product_list, product_dir, type_prefix, dir_type):
    product_list.append(
        {
            "remote_path": f"irods:{product_dir}",
            "file_type": f"{type_prefix}_{dir_type}",
        }
    )


def sub_collection_names(coll):
    top_types = set()  # Set of file types found at the top level
    sub_names = set()  # Set of subdirectory names found
    for obj in coll.contents():
        if isinstance(obj, Collection):
            sub_names.add(obj.path.name)
        elif m := re.search(r"\.(\w+)(\.gz)?$", obj.name, re.IGNORECASE):
            # Upper-cased file extension, ignoring any ".gz" suffix
            top_types.add(m.group(1).upper())

    logging.debug(
        f"In {coll.path} found:"
        f"\n  top_types = {sorted(top_types)}"
        f"\n  sub_names = {sorted(sub_names)}"
    )

    return top_types, sub_names


def build_run_id(avu_dict):
    # Avoid using run IDs which are just an integer
    run_id = avu_dict.get("experiment_name")
    if re.fullmatch(r"\d+", run_id):
        run_id = f"ONT-EARLY-{run_id}"

    return run_id


def build_data_id(avu_dict, run_id=None):
    data_id_components = [run_id or build_run_id(avu_dict)]
    for field in (
        "flowcell_id",
        "instrument_slot",
        "tag_identifier",
        "tag2_identifier",
    ):
        if cmpt := avu_dict.get(field):
            data_id_components.append(str(cmpt))

    return "#".join(data_id_components) if data_id_components else None


def fetch_mlwh_info(sample_name_list, page_size=100):
    mlwh = db_connection.mlwh_db()

    crsr = mlwh.cursor(dictionary=True)
    last_count = None
    sql = None
    for i in range(0, len(sample_name_list), page_size):
        book = sample_name_list[i : i + page_size]
        count = len(book)
        if count != last_count:
            sql = mlwh_ont_info_sql(count)
            logging.debug(sql)
        crsr.execute(sql, book)
        for row in crsr:
            run_id = build_run_id(row)
            data_id = build_data_id(row, run_id)
            row["data_id"] = data_id
            row["run_id"] = run_id
            yield row


def mlwh_ont_info_sql(count):
    placeholders = ", ".join(["%s"] * count)

    return inspect.cleandoc(f"""
        SELECT
            sample.name AS sample_name
          , sample.supplier_name AS supplier_name
          , sample.public_name AS tol_specimen_id
          , sample.accession_number AS biosample_accession
          , sample.donor_id AS biospecimen_accession
          , sample.common_name AS scientific_name
          , sample.taxon_id
          , flowcell.experiment_name
          , flowcell.flowcell_id
          , flowcell.instrument_slot
          , flowcell.tag_identifier
          , flowcell.tag2_identifier
          , flowcell.instrument_name
          , COALESCE(flowcell.pipeline_id_lims, 'ONT') AS pipeline_id_lims
          , COALESCE(flowcell.library_tube_barcode, sample.name) AS library_id
        FROM sample
        LEFT JOIN oseq_flowcell AS flowcell USING (id_sample_tmp)
        WHERE sample.name IN ({placeholders})
    """)  # noqa: S608


def add_mlwh_sample_data(ont_rows):
    sample_name_list = sample_names_from_rows(ont_rows)

    mlwh_by_sample = {}
    mlwh_by_data_id = {}
    for row in fetch_mlwh_info(sample_name_list):
        if sample_name := row.get("sample_name"):
            mlwh_by_sample[sample_name] = row
        if data_id := row.get("data_id"):
            mlwh_by_data_id[data_id] = row

    for row in ont_rows:
        merge_mlwh_data(row, mlwh_by_data_id, mlwh_by_sample)


def sample_names_from_rows(ont_rows):
    sn_set = set()
    for row in ont_rows:
        if sample_name := row.get("sample_name"):
            sn_set.add(sample_name)
    return sorted(sn_set)


SAMPLE_FIELDS = (
    "supplier_name",
    "tol_specimen_id",
    "biosample_accession",
    "biospecimen_accession",
    "scientific_name",
    "taxon_id",
)
DATA_ID_FIELDS = (
    *SAMPLE_FIELDS,
    "instrument_name",
    "pipeline_id_lims",
    "library_id",
)


def merge_mlwh_data(row, mlwh_by_data_id, mlwh_by_sample):
    """
    MLWH may be a little more up to date than iRODS, so we replace any iRODS
    fields with MLWH fields which have values.
    """

    if (data_id := row.get("data_id")) and (mlwh_row := mlwh_by_data_id.get(data_id)):
        # data_id matches, so can take values from both the sample and
        # oseq_flowcell MLWH tables.
        merge_data(row, mlwh_row, DATA_ID_FIELDS)
    elif (sample_name := row.get("sample_name")) and (
        mlwh_row := mlwh_by_sample.get(sample_name)
    ):
        # Only the sample name matches so only take sample table values
        merge_data(row, mlwh_row, SAMPLE_FIELDS)


def merge_data(row, mlwh_row, fields):
    for fld in fields:
        val = mlwh_row.get(fld)
        if val is not None:
            row[fld] = val


def store_ont_data(client, ont_rows):
    mlwh_rows = []
    data_files = []
    max_modified = None
    study_data_ids = {}  # List of data_id in each study
    for row in ont_rows:
        data_id = row["data_id"]
        study_data_ids.setdefault(row["study_id"], []).append(data_id)
        store = {}
        for k, v in row.items():
            if k == "files":
                for file in v:
                    data_files.append(
                        {
                            "data.id": data_id,
                            **file,
                        }
                    )
            else:
                store[k] = v
                if k == "qc_date" and (not max_modified or v > max_modified):
                    max_modified = v
        mlwh_rows.append(store)

    # Prepare any ONT file entries whose "remote_path" values aren't in the
    # database for storing.
    upsrtr = TableUpserter(client)
    upsrtr.build_table_upserts("file", data_files, key="remote_path")

    # Create or update the sequence data
    rspns = client.ndjson_post("loader/seq-data", [ndjson_row(x) for x in mlwh_rows])

    # Store new file table entries and index them by data_id
    data_id_files = {}
    for file in upsrtr.apply_upserts():
        data_id_files.setdefault(file.data.id, []).append(file)

    # Build dicts of new and updated data indexed by data_id
    new_by_data_id = response_field_dict_by_data_id(rspns, "new")
    upd_by_data_id = response_field_dict_by_data_id(rspns, "updated")

    print_report(study_data_ids, data_id_files, new_by_data_id, upd_by_data_id)

    if max_modified:
        update_stored_max_modified(client, max_modified)


def print_report(study_data_ids, data_id_files, new_by_data_id, upd_by_data_id):
    """
    Prints a report of new and updated ONT data grouped by study
    """

    out = sys.stdout
    for study_id, data_id_list in study_data_ids.items():
        new_data = []
        upd_data = []
        for data_id in data_id_list:
            if new := new_by_data_id.get(data_id):
                new_data.append(new)
            elif upd := upd_by_data_id.get(data_id):
                upd_data.append(upd)

        if new_data:
            study_name = new_data[0]["study"]
            out.write(f"\n\nNew ONT data in '{study_name} ({study_id})':\n\n")
            for row in new_data:
                out.write(response_row_std_fields(row))
                if file_list := data_id_files.get(row["data_id"]):
                    for file in file_list:
                        out.write(f"  New data: {file.file_type}\n")

        if upd_data:
            study_name = upd_data[0]["study"]
            out.write(f"\n\nUpdated ONT data in '{study_name} ({study_id})':\n\n")
            for row in upd_data:
                out.write(response_row_std_fields(row))
                for col, (old_val, new_val) in row["changes"].items():
                    out.write(f"  {col} changed from {old_val!r} to {new_val!r}\n")
                if file_list := data_id_files.get(row["data_id"]):
                    for file in file_list:
                        out.write(f"  New data: {file.file_type}\n")


def response_field_dict_by_data_id(rspns, fld):
    by_data_id = {}
    if fld_list := rspns.get(fld):
        for row in fld_list:
            by_data_id[row["data_id"]] = row
    return by_data_id
