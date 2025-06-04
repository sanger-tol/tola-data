import sys
from pathlib import Path

import click

from tola import click_options
from tola.ndjson import (
    ndjson_row,
    parse_ndjson_stream,
    pretty_row,
)
from tola.pretty import colour_pager
from tola.subtrack import SubTrack
from tola.terminal import pretty_dict_itr
from tola.tqc.engine import guess_file_type, parse_id_list_stream


@click.command()
@click.option(
    "--key",
    default="remote_path",
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
@click_options.file
@click_options.file_format
@click.argument(
    "data-filenames",
    nargs=-1,
    required=False,
)
def subtrack(key, throw_if_missing, file_list, file_format, data_filenames):
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

    query_obj = get_file_name_query_objects(key, data_filenames, file_list, file_format)
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

    if sys.stdout.isatty():
        colour_pager(pretty_dict_itr(subtrack_info, key))
    else:
        for info in subtrack_info:
            sys.stdout.write(ndjson_row(info))


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
