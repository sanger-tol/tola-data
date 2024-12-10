import pathlib
import sys

import click
from ulid import ULID

from tola import click_options
from tola.ndjson import ndjson_row
from tola.pretty import bold, s
from tola.tqc.engine import input_objects_or_exit


@click.command()
@click.pass_context
@click.option(
    "--output",
    "-o",
    type=click.Path(
        path_type=pathlib.Path,
    ),
    default=pathlib.Path(),
    show_default=True,
    help=(
        """
        Location the output. If the location is a directory, a file containing
        the dataset info named "datasets.ndjson" will be created or appended
        to.

        If it is a file, a file of that name will be created or appended to.

        If it is a dash character, ND-JSON will be printed to STDOUT.

        Alternatively an "output" location can be specified in each line of
        the ND-JSON input.
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
def dataset(ctx, output, noisy, input_files):
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

    stored_datasets = {}
    input_obj = input_objects_or_exit(ctx, input_files)
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

    if noisy:
        echo_datasets(stored_datasets)


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
        # Write dataset info to file
        if label == "new":
            for dsr in ds_rows:
                file.write(ndjson_row(dsr))
        stored_datasets.setdefault(label, []).extend(ds_rows)


def echo_datasets(stored_datasets):
    for label in sorted(stored_datasets):
        stored = stored_datasets[label]
        click.echo(f"\n{bold(len(stored))} {label} dataset{s(stored)}:", err=True)
        for ds in stored:
            click.echo(f"  {bold(ds['dataset.id'])}", err=True)
            for ele in ds["elements"]:
                click.echo(f"    {ele['data.id']}", err=True)


def count_output_field(input_obj):
    n = 0
    for obj in input_obj:
        if obj.get("output"):
            n += 1
    return n
