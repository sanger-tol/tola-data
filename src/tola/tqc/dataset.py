import sys
from pathlib import Path

import click
from ulid import ULID

from tola import click_options
from tola.ndjson import ndjson_row, parse_ndjson_stream
from tola.pretty import bold, colour_pager, natural, s
from tola.terminal import pretty_dict_itr
from tola.tqc.engine import fetch_list_or_exit, input_objects_or_exit


@click.command()
@click.pass_context
@click.option(
    "--info/--no-info",
    "-i",
    "info_flag",
    default=False,
    show_default=True,
    help=(
        """
        Show information about the current (last, latest) dataset for any of
        the files or directories in the INPUT_FILES arguments, or the current
        directory if none are given.
        """
    ),
)
@click.option(
    "--output",
    "-o",
    type=click.Path(
        path_type=Path,
        exists=True,
        allow_dash=True,
    ),
    default=Path(),
    show_default=True,
    help=(
        """
        Location the output. If the location is a directory, a file containing
        the dataset info named "datasets.ndjson" will be created or appended
        to.

        If it is a file, a file of that name will be created or appended to.
        Only newly created datasets will be written.

        If it is a dash character, ND-JSON will be printed to STDOUT. This
        will write both newly created and existing dataset to STDOUT.

        Alternatively an "output" location can be specified in each line of
        the ND-JSON input.
        """
    ),
)
@click.option(
    "--fofn",
    "-f",
    "fofn_paths",
    type=click.Path(
        path_type=Path,
        exists=True,
        allow_dash=True,
    ),
    multiple=True,
    help=(
        """
        File Of File Names input from which to create a single dataset. Can be
        specified multiple times. Each line within the files becomes
        a "remote_path" in the list of elements of the dataset.

        PATH can be a file or a directory.

        If the PATH is a directory, it will be recursively searched for files
        matching the pattern "IRODS.*.fofn", all of which will be used to
        build the list of "remote_path" elements for the dataset.

        If PATH is a dash character, STDIN will be used.
        """
    ),
)
@click.option(
    "--noisy/--quiet",
    default=True,
    show_default=True,
    help="List new and existing datasets to STDERR",
)
@click_options.input_files
def dataset(ctx, info_flag, output, fofn_paths, noisy, input_files):
    """
    Store new datasets, populating the `dataset` and `dataset_element` tables
    in the ToLQC database, and giving each newly created dataset a current
    status of "Pending". ND-JSON input files should contain data structured
    as:

      {"elements": [...], "output": <str>}

    The format of each item in the "elements" list is:

      {"data.id": <str>, "remote_path": <str>}

    where either "data.id" or "remote_path" must be specified. Each element is
    resolved to an existing `data.data_id` via the supplied "data.id", or via
    `file.remote_path` if "remote_path" is supplied but "data.id" is not.

    The output location can be specified either by the "--output" command line
    option, or individually for each row in the ND-JSON in an "output"
    field.

    A "dataset.id" field can also be included, but a ULID will otherwise be
    automatically generated.
    """

    client = ctx.obj

    if info_flag:
        if not input_files:
            # Default to the current directory
            input_files = [Path()]
        found_datasets = []
        for info in input_files:
            ds_file = info if info.is_file() else find_dataset_file(info)
            if not ds_file:
                continue
            if latest := latest_dataset(ds_file):
                found_datasets.append(latest)
        print_dataset_info(client, found_datasets)
    else:
        stored_datasets = {}
        input_obj = (
            input_objects_from_fofn_or_exit(fofn_paths)
            if fofn_paths
            else input_objects_or_exit(ctx, input_files)
        )
        if out_count := count_output_field(input_obj):
            # Check that all the input rows have "output" set
            if out_count != len(input_obj):
                sys.exit(
                    f"Only {out_count} of {len(input_obj)}"
                    ' input rows have the "output" field set'
                )

            # Store datasets one-by-one to the server, since the output file for
            # any row might be unwriteable.
            for obj in input_obj:
                row_output = obj.pop("output")
                store_dataset_rows(client, row_output, (obj,), stored_datasets)
        else:
            store_dataset_rows(client, output, input_obj, stored_datasets)

        if noisy and str(output) != "-":
            echo_datasets(stored_datasets)


def latest_dataset_id(path: Path):
    ds_dir = path if path.is_dir() else path.parent
    if (ds_file := find_dataset_file(ds_dir)) and (latest := latest_dataset(ds_file)):
        return latest["dataset.id"]
    return None


def latest_dataset(ds_file):
    latest = None
    for ds in parse_ndjson_stream(ds_file.open()):
        # `latest` will be set to the last dataset in the file
        latest = ds
    return latest


def find_dataset_file(directory: Path):
    """Searches up the directory path for file named `datasets.ndjson`"""
    look = directory.absolute()
    found = None
    while not found:
        dsf = look / "datasets.ndjson"
        if dsf.exists():
            found = dsf
        elif str(look) == look.root:
            break
        else:
            look = look.parent
    return found


def input_objects_from_fofn_or_exit(fofn_paths):
    input_obj = input_objects_from_fofn(fofn_paths)
    if not input_obj:
        sys.exit("Error: No remote paths from --fofn input")
    return input_obj


def input_objects_from_fofn(fofn_paths):
    remote_paths = []
    for fofn in fofn_paths:
        if str(fofn) == "-":
            remote_paths.extend(lines_from_filehandle(sys.stdin))
        elif fofn.is_dir():
            for fofn_file in fofn.rglob("IRODS.*.fofn"):
                remote_paths.extend(lines_from_filehandle(fofn_file.open()))
        else:
            remote_paths.extend(lines_from_filehandle(fofn.open()))

    return (
        [{"elements": [{"remote_path": r} for r in remote_paths]}]
        if remote_paths
        else None
    )


def lines_from_filehandle(fh):
    return [line.strip() for line in fh]


def store_dataset_rows(client, output, rows, stored_datasets):
    if str(output) == "-":
        file = sys.stdout
    else:
        file_path = output / "datasets.ndjson" if output.is_dir() else output
        file = file_path.open("a")

    # Add ULID dataset.id to any row without a dataset.id
    for dsr in rows:
        if not dsr.get("dataset.id"):
            dsr["dataset.id"] = str(ULID())

    # Store datasets and record response in stored_datasets dict
    rspns = client.ndjson_post("loader/dataset", (ndjson_row(x) for x in rows))

    for label, ds_rows in rspns.items():
        # If writing to STDOUT, print all new or existing dastasets.
        # Otherwise only append new datasets to file.
        if file == sys.stdout or label == "new":
            for dsr in ds_rows:
                file.write(ndjson_row(dsr))
        stored_datasets.setdefault(label, []).extend(ds_rows)


def echo_datasets(stored_datasets):
    def ce(msg):
        click.echo(msg, err=True)

    for label in sorted(stored_datasets):
        stored = stored_datasets[label]
        ce(f"\n{bold(len(stored))} {label} dataset{s(stored)}:")
        for ds in stored:
            ce(f"\n  {bold(ds['dataset.id'])}")
            for ele in ds["elements"]:
                ce(f"    {ele['data.id']}")


def print_dataset_info(client, found_datasets):
    key = "dataset.id"
    db_datasets = fetch_list_or_exit(
        client, "dataset", key, [x[key] for x in found_datasets]
    )
    display = []
    for fnd, db_obj in zip(found_datasets, db_datasets, strict=True):
        status = db_obj.status
        elements = fnd["elements"]
        db_elements = fetch_list_or_exit(
            client, "data", "data.id", [x["data.id"] for x in elements]
        )
        specimens = sorted({x.sample.specimen.id for x in db_elements}, key=natural)
        display.append(
            {
                "dataset.id": fnd[key],
                "status": status.status_type.id,
                "status_time": status.status_time,
                "specimens": specimens,
                "elements": [
                    {
                        "library_type": x.library.library_type.id,
                        "data.id": x.id,
                    }
                    for x in db_elements
                ],
            }
        )

    if sys.stdout.isatty():
        colour_pager(
            pretty_dict_itr(display, None, head="Information for {} dataset{}:")
        )
    else:
        for row in display:
            sys.stdout.write(ndjson_row(row))


def count_output_field(input_obj):
    n = 0
    for obj in input_obj:
        if obj.get("output"):
            n += 1
    return n
