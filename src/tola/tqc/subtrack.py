import sys
from pathlib import Path

import click

from tola import click_options
from tola.ndjson import ndjson_row
from tola.pretty import colour_pager
from tola.subtrack import SubTrack
from tola.terminal import pretty_dict_itr
from tola.tqc.engine import id_iterator


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

    e.g. tqc subtrack 36703_6#11.cram m84047_240704_124657_s2.hifi_reads.bc2070.bam
    """

    ### Add data_id to output

    name_list = tuple(
        Path(x).name for x in id_iterator(key, data_filenames, file_list, file_format)
    )
    fetched_info = {
        x["file_name"]: x for x in SubTrack().fetch_submission_info(name_list)
    }

    subtrack_info = []
    not_found = []
    for n in name_list:
        if info := fetched_info.get(n):
            subtrack_info.append(info)
        else:
            not_found.append(n)

    if throw_if_missing and not_found:
        nf_list = "".join(f"  {x}\n" for x in not_found)
        sys.exit("Failed to fetch info from subtrack for files:\n" + nf_list)

    if sys.stdout.isatty():
        colour_pager(pretty_dict_itr(subtrack_info, key))
    else:
        for info in subtrack_info:
            sys.stdout.write(ndjson_row(info))
