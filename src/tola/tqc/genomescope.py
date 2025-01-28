import json
import sys
from pathlib import Path

import click

from tola.pretty import bold, s
from tola.tqc.dataset import latest_dataset_id


class StoreGenomescopeError(Exception):
    """Failure to store a genomescope result"""


@click.command()
@click.pass_context
@click.option(
    "--dataset-id",
    required=False,
    help="A dataset.id to store a genomescope result under",
)
@click.option(
    "--rerun-if-no-json",
    flag_value=True,
    default=False,
    help=(
        """
        If there is no `results.json` file, then rerun genomescope using the
        parameters found in the existing `summary.txt` file.
        """
    ),
)
@click.argument(
    "input_dirs",
    nargs=-1,
    required=False,
    type=click.Path(
        path_type=Path,
        exists=True,
        readable=True,
    ),
)
def genomescope(ctx, dataset_id, rerun_if_no_json, input_dirs):
    """
    Load genomescope2.0 results into TolQC.

    The genomescope results files in each directory given in INPUT_DIRS will
    be scanned and stored under the dataset.id
    """
    client = ctx.obj

    if dataset_id and len(input_dirs) != 1:
        sys.exit(
            f"dataset.id set to '{dataset_id}' but {len(input_dirs)} INPUT_DIRS given."
            " Can only specify one input directory if a --dataset-id argument is set"
        )

    failures = []
    for rdir in input_dirs:
        try:
            store_genomescope_results(client, rdir, dataset_id, rerun_if_no_json)
        except StoreGenomescopeError as gsf:
            (msg,) = gsf.args
            failures.append(msg)

    if sys.stdout.isatty():
        success = len(input_dirs) - len(failures)
        if success:
            click.echo(
                f"Stored {bold(success)} genomescope result{s(success)}",
                err=True,
            )

    if fc := len(failures):
        sys.exit(
            "\n  ".join(
                [
                    (
                        "Failed to store genomescope results for"
                        f" {fc} input director{'y' if fc == 1 else 'ies'}:"
                    ),
                    *failures,
                ]
            )
        )


def store_genomescope_results(
    client,
    rdir: Path,
    dataset_id=None,
    rerun_if_no_json=False,
):
    if not dataset_id:
        dataset_id = latest_dataset_id(rdir)
        if not dataset_id:
            msg = (
                "Failed to find dataset_id from a 'datasets.ndjson'"
                f" file in or above directory '{rdir}'"
            )
            raise StoreGenomescopeError(msg)
    report = report_json_contents(rdir)
    click.echo(json.dumps(report, indent=2), err=True)

    fldr = client.get_folder_location("genomescope_s3")


def report_json_contents(rdir: Path):
    # Find report.json file
    report_file = None
    for rf in rdir.glob("*report.json"):
        report_file = rf
    if not report_file:
        msg = f"Missing `report.json` file in directory '{rdir}'"
        raise StoreGenomescopeError(msg)

    return json.loads(report_file.read_text())
