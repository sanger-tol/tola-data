import sys
from pathlib import Path
from typing import Any

import click

from tola import click_options
from tola.ndjson import (
    ndjson_row,
    parse_ndjson_stream,
    pretty_row,
)
from tola.pretty import plain_text_from_itr
from tola.subtrack import SubTrack
from tola.terminal import colour_pager, pretty_dict_itr
from tola.tolqc_client import TolClient
from tola.tqc.engine import (
    dicts_to_core_data_objects,
    guess_file_type,
    parse_id_list_stream,
)


@click.command()
@click.pass_context
@click.option(
    "--auto",
    "auto_flag",
    is_flag=True,
    default=False,
    show_default=True,
    help="""
      Query subtrack with any "Pending" accessions found in the ToLQC `data`
      table, fill in the `data_submission` table with those records, populate
      the `accession` table, and update `data.accession.id` with run
      accessions.
    """,
)
@click.option(
    "--auto-value",
    default="Pending",
    show_default=True,
    help="""
      Value to search for in `data.accession.id` to select entries.  Set
      to "null" to search for all "null" accessions (which may be useful to
      fill in data for faculty projects).
    """,
)
@click.option(
    "--key",
    default="file_name",
    show_default=True,
    help="Name of column in input containg data filenames.",
)
@click.option(
    "--throw",
    "throw_if_missing",
    help="Exit with an error if records for any of the filenames are not found",
    is_flag=True,
    default=False,
    show_default=True,
)
@click_options.pretty
@click_options.file
@click_options.file_format
@click.argument(
    "data-filenames",
    nargs=-1,
    required=False,
)
def subtrack(
    ctx,
    auto_flag,
    auto_value,
    key,
    throw_if_missing,
    pretty,
    file_list,
    file_format,
    data_filenames,
):
    """Show information from the subtrack database

    DATA_FILENAMES is a list of data filenames to fetch ENA submission
    tracking information on, which can additionally be provided in --file
    arugments, or alternatively piped to STDIN. Each element is treated as a
    path and the filename component is parsed from it.

    Output is in human readable format if STOUT is a terminal, or ND-JSON if
    redirected to a file or UNIX pipe.

    `data_id` or `data.id` fields in ND-JSON input are preserved in the
    output.

    e.g. tqc subtrack 36703_6#11.cram m84047_240704_124657_s2.hifi_reads.bc2070.bam
    """

    client = ctx.obj
    if auto_flag:
        if key != "file_name":
            sys.exit("Cannot set --key to {key!r} with --auto")
        auto_value = None if auto_value.lower() == "null" else auto_value
        query_obj = get_file_names_by_accession_value(client, auto_value)
        if not query_obj:
            sys.exit(0)
    else:
        query_obj = get_file_name_query_objects(
            key, data_filenames, file_list, file_format
        )
        if not query_obj:
            sys.exit("No input provided")

    fetched_info = {
        x["file_name"]: x
        for x in SubTrack().fetch_submission_info([n["file_name"] for n in query_obj])
    }

    subtrack_info = []
    not_found = []
    for obj in query_obj:
        if info := fetched_info.get(obj["file_name"]):
            subtrack_info.append(obj | info)
        else:
            not_found.append(obj)

    if throw_if_missing and not_found:
        nf_list = "".join(f"  {x['file_name']}\n" for x in not_found)
        sys.exit("Failed to fetch info from subtrack for files:\n" + nf_list)

    if auto_flag:
        # Filter out entries without run accessions
        subtrack_info = [x for x in subtrack_info if x["run_accession"] is not None]
        store_accessions(client, subtrack_info)
        store_data_submissions(client, subtrack_info)
        update_data_table_run_accessions(client, subtrack_info)

    if not subtrack_info:
        sys.exit(0)

    if pretty or sys.stdout.isatty():
        head = (
            "Fetched {} new submission records from SubTrack:"
            if auto_flag
            else "Found {} SubTrack records:"
        )
        itr = pretty_dict_itr(subtrack_info, key, head=head)
        if pretty:
            print(plain_text_from_itr(itr))
        else:
            colour_pager(itr)
    else:
        for info in subtrack_info:
            sys.stdout.write(ndjson_row(info))


def store_accessions(client: TolClient, subtrack_info: list[dict[str, Any]]) -> None:
    acc_upserts = {}
    for info in subtrack_info:
        for acc_info in [
            {
                "accession.id": info["run_accession"],
                "accession_type_id": "Run",
                "secondary": info["experiment_accession"],
                "submission": info["submission_accession"],
                "date_submitted": info["submission_time"],
            },
            {
                "accession.id": info["experiment_accession"],
                "accession_type_id": "Experiment",
                "secondary": info["run_accession"],
                "submission": info["submission_accession"],
                "date_submitted": info["submission_time"],
            },
            {
                "accession.id": info["sample_accession"],
                "accession_type_id": "BioSample",
            },
            {
                "accession.id": info["study_accession"],
                "accession_type_id": "BioProject",
            },
        ]:
            acc_upserts[acc_info["accession.id"]] = acc_info

    ads = client.ads
    acc_cdo_list = dicts_to_core_data_objects(ads, "accession", acc_upserts.values())
    ads.upsert("accession", acc_cdo_list)


def store_data_submissions(
    client: TolClient, subtrack_info: list[dict[str, Any]]
) -> None:
    ads = client.ads
    data_sub_cdo_list = dicts_to_core_data_objects(
        ads,
        "data_submission",
        [
            {
                "data_submission.id": info["data_id"],
                "run_accession_id": info["run_accession"],
                "study_accession_id": info["study_accession"],
                "sample_accession_id": info["sample_accession"],
                "experiment_accession_id": info["experiment_accession"],
                "submission_time": info["submission_time"],
            }
            for info in subtrack_info
        ],
    )
    ads.upsert("data_submission", data_sub_cdo_list)


def update_data_table_run_accessions(
    client: TolClient, subtrack_info: list[dict[str, Any]]
) -> None:
    ads = client.ads
    data_cdo_list = dicts_to_core_data_objects(
        ads,
        "data",
        [
            {
                "data.id": info["data_id"],
                "accession_id": info["run_accession"],
            }
            for info in subtrack_info
        ],
    )
    ads.upsert("data", data_cdo_list)


def get_file_names_by_accession_value(client: TolClient, auto_value: str | None):
    query_obj = []
    for file in client.ads_get_list(
        "file",
        filter_spec={
            # These submission file types should probably go in the `metadata` table.
            "file_type": {"in_list": {"value": ["BAM", "CRAM", "RECALL_BAM"]}},
            "data.accession.id": {"eq": {"value": auto_value}},
        },
    ):
        path = file.name or file.remote_path
        if not path:
            continue
        file_name = Path(path).name
        query_obj.append(
            {
                "file_name": file_name,
                "data_id": file.data.id,  # ty: ignore[unresolved-attribute]
            }
        )

    return query_obj


def get_file_name_query_objects(key, data_filenames, file_list, file_format):
    if data_filenames:
        return [{"file_name": x} for x in data_filenames]

    query_obj = []
    if file_list:
        for file in file_list:
            fmt = file_format or guess_file_type(file)
            with file.open() as fh:
                query_obj.extend(
                    query_obj_from_id_list_fh(fh)
                    if fmt == "TXT"
                    else query_obj_from_ndjson_fh(fh, key)
                )
    elif not sys.stdin.isatty():
        query_obj.extend(
            query_obj_from_id_list_fh(sys.stdin)
            if file_format == "TXT"
            else query_obj_from_ndjson_fh(sys.stdin, key)
        )

    return query_obj


def query_obj_from_ndjson_fh(fh, key):
    for obj in parse_ndjson_stream(fh):
        pth = obj.get(key)
        if not pth:
            sys.exit(f"Missing '{key}' field in object:\n" + pretty_row(obj))
        inp = {"file_name": Path(pth).name}
        for fld in "data_id", "data.id":
            if val := obj.get(fld):
                inp[fld] = val
        yield inp


def query_obj_from_id_list_fh(fh):
    for pth in parse_id_list_stream(fh):
        yield {"file_name": Path(pth).name}
