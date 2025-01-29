import json
import sys
from pathlib import Path

import click

from tola.ndjson import ndjson_row
from tola.pretty import colour_pager
from tola.store_folder import upload_files
from tola.tqc.dataset import latest_dataset_id
from tola.tqc.engine import core_data_object_to_dict, pretty_dict_itr


class StoreGenomescopeError(Exception):
    """Failure to store a genomescope result"""


@click.command()
@click.pass_context
@click.option(
    "--dataset-id",
    required=False,
    help=(
        """
        An optional dataset.id to store a single genomescope result under if
        the results directory is not under one containing a "datasets.ndjson"
        file.
        """
    ),
)
@click.option(
    "--rerun-if-no-json",
    flag_value=True,
    default=False,
    help=(
        """
        If there is no 'results.json' file, then rerun genomescope using the
        parameters found in the existing 'summary.txt' file.
        """
    ),
)
@click.option(
    "--folder-location",
    "folder_location_id",
    default="genomescope_s3",
    show_default=True,
    help="Folder location to save image and histogram data files to",
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
def genomescope(ctx, dataset_id, rerun_if_no_json, folder_location_id, input_dirs):
    """
    Load genomescope2.0 results into TolQC.

    The genomescope results files in each directory given in INPUT_DIRS will
    be scanned and stored under a dataset.id

    The dataset.id for each directory is automatically determined from the
    nearest "datasets.ndjson" within its hierachcy.
    """
    client = ctx.obj

    if dataset_id and len(input_dirs) != 1:
        sys.exit(
            f"dataset.id set to '{dataset_id}' but {len(input_dirs)} INPUT_DIRS given."
            " Can only specify one input directory if a --dataset-id argument is set"
        )

    results = []
    failures = []
    for rdir in input_dirs:
        try:
            rslt = store_genomescope_results(
                client,
                rdir,
                dataset_id,
                rerun_if_no_json,
                folder_location_id,
            )
        except StoreGenomescopeError as gsf:
            (msg,) = gsf.args
            failures.append(msg)
            continue
        results.append(rslt)

    success = len(input_dirs) - len(failures)
    if success:
        if sys.stdout.isatty():
            colour_pager(
                pretty_dict_itr(
                    results,
                    "genomescope_metrics.id",
                    head="Stored {} genomescope result{}",
                )
            )
        else:
            for rslt in results:
                sys.stdout.write(ndjson_row(rslt))

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
    folder_location_id="genomescope_s3",
):
    tbl_name = "genomescope_metrics"

    if not dataset_id:
        dataset_id = latest_dataset_id(rdir)
        if not dataset_id:
            msg = (
                "Failed to find dataset_id from a 'datasets.ndjson'"
                f" file in or above directory '{rdir}'"
            )
            raise StoreGenomescopeError(msg)

    report = report_json_contents(rdir, rerun_if_no_json)
    attr = attr_from_report(report)
    attr["dataset_id"] = dataset_id

    # Store GenomescopeMetrics
    ads = client.ads
    (gsm,) = ads.upsert(tbl_name, [ads.data_object_factory(tbl_name, attributes=attr)])

    # Store genomescope images and histogram data
    files = upload_files(
        client,
        folder_location_id,
        tbl_name,
        {
            f"{tbl_name}.id": gsm.id,
            "directory": rdir,
        },
    )

    # Make a flattened dict of the result and merge in the dict from
    # `upload_files()`
    rslt = core_data_object_to_dict(gsm)
    for k, v in files.items():
        if k == "id_key":
            continue
        rslt[k] = v
    return rslt


def attr_from_report(report):
    """
    Extracts the attributes for the genomescope_metrics columns from the JSON report
    """
    param = report["input_parameters"]
    return {
        # Input parameters
        "kmer": param["kmer_length"],
        "ploidy": param["ploidy"],
        "kcov_init": param["est_kmer_coverage"],
        # Results
        "homozygous": report["homozygous"]["avg"],
        "heterozygous": report["heterozygous"]["avg"],
        "haploid_length": report["genome_haploid_length"]["avg"],
        "unique_length": report["genome_unique_length"]["avg"],
        "repeat_length": report["genome_repeat_length"]["avg"],
        "kcov": report["kcov"],
        "model_fit": report["model_fit"]["full"],
        "read_error_rate": report["read_error_rate"],
        # Full report is stored as JSON in the `results` column
        "results": report,
    }


def report_json_contents(rdir: Path, rerun_if_no_json=False):
    # Find report.json file
    report_file = None
    for rf in rdir.glob("*report.json"):
        if report_file:
            msg = f"More than one 'report.json' in '{rdir}': '{report_file}' and '{rf}'"
            raise StoreGenomescopeError(msg)
        report_file = rf
    if not report_file:
        if rerun_if_no_json:
            pass
        else:
            msg = f"Missing report.json file in directory '{rdir}'"
            raise StoreGenomescopeError(msg)

    return json.loads(report_file.read_text())
