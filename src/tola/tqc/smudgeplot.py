import logging
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path

import click

from tola.filesystem import (
    TolFileSystemError,
    file_json_contents,
    find_file,
    find_file_or_raise,
    latest_dataset_id_or_raise,
)
from tola.ndjson import ndjson_row
from tola.store_folder import upload_files
from tola.terminal import colour_pager, pretty_dict_itr
from tola.tolqc_client import TolClient
from tola.tqc.engine import core_data_object_to_dict

SMUDGE_ROOT = "fastk_smudgeplot"


class SmudgeplotError(Exception):
    """Failure to run smudgeplot or storing a smudgeplot result"""


@click.command()
@click.pass_context
@click.option(
    "--run/--upload",
    "run_flag",
    default=True,
    show_default=True,
    help=(
        """
        Whether to run smudgeplot in each directory in INPUT_DIRS or upload
        existing smudgeplot results to the ToLQC database.
        """
    ),
)
@click.option(
    "--dataset-id",
    "cli_dataset_id",
    required=False,
    help=(
        """
        An optional dataset.id to store a single smudgeplot result under if
        the results directory is not under one containing a "datasets.ndjson"
        file.
        """
    ),
)
@click.option(
    "--smudgeplot-cmd",
    default="smudgeplot",
    show_default=True,
    help=(
        """
        Command for running smudgeplot.
        """
    ),
)
@click.option(
    "--threshold",
    type=int,
    default=10,
    show_default=True,
    help=(
        """
        Threshold passed to the `smudgeplot hetmers` command, the count below
        which K-mers are considered erroneous.
        """
    ),
)
@click.option(
    "--min-coverage",
    type=int,
    help=(
        """
        Smudgeplot min_cov parameter.
        """
    ),
)
@click.option(
    "--max-coverage",
    type=int,
    help=(
        """
        Smudgeplot max_cov parameter.
        """
    ),
)
@click.option(
    "--folder-location",
    "folder_location_id",
    default="smudgeplot_s3",
    show_default=True,
    help="Folder location to save image and histogram data files to.",
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
def smudgeplot(
    ctx,
    run_flag,
    cli_dataset_id,
    threshold,
    min_coverage,
    max_coverage,
    folder_location_id,
    smudgeplot_cmd,
    input_dirs,
):
    """
    Load smudgeplot results into TolQC.

    The smudgeplot results files in each directory given in INPUT_DIRS will be
    scanned and stored under the `dataset.id` from the
    nearest "datasets.ndjson" file in or above it in the filesystem tree.
    """
    client: TolClient = ctx.obj

    if cli_dataset_id and len(input_dirs) != 1:
        sys.exit(
            f"dataset.id set to '{cli_dataset_id}' but {len(input_dirs)} INPUT_DIRS"
            " given.  Can only specify one input directory if a --dataset-id argument"
            " is set."
        )

    if (threshold, min_coverage or max_coverage) and not run_flag:
        sys.exit(
            "The --threshold, --min-coverage or --max-coverage options are only used"
            " when the --run flag is set."
        )

    results = []
    failures = []

    for rdir in input_dirs:
        try:
            dataset_id = (
                latest_dataset_id_or_raise(rdir)
                if cli_dataset_id is None
                else cli_dataset_id
            )
            if run_flag:
                report_file = new_smudgeplot_run(
                    rdir,
                    smudgeplot_cmd=smudgeplot_cmd,
                    threshold=threshold,
                    min_coverage=min_coverage,
                    max_coverage=max_coverage,
                )
            else:
                report_file = find_report_file(rdir)

            if not report_file:
                msg = f"Missing report.json file in directory '{rdir}'"
                raise SmudgeplotError(msg)

            rslt = store_smudgeplot_results(
                client,
                rdir,
                dataset_id,
                report_file,
                folder_location_id=folder_location_id,
            )

        except (SmudgeplotError, TolFileSystemError) as spe:
            (msg,) = spe.args
            failures.append(msg)
            continue
        results.append(rslt)

    success = len(input_dirs) - len(failures)
    if success:
        if sys.stdout.isatty():
            colour_pager(
                pretty_dict_itr(
                    results,
                    "smudgeplot_metrics.id",
                    head="Stored {} smudgeplot result{}",
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
                        "Failed to store smudgeplot results for"
                        f" {fc} input director{'y' if fc == 1 else 'ies'}:"
                    ),
                    *failures,
                ]
            )
        )


def store_smudgeplot_results(
    client: TolClient,
    rdir: Path,
    dataset_id: str,
    report_file: Path,
    *,
    folder_location_id="smudgeplot_s3",
):
    tbl_name = "smudgeplot_metrics"

    report = file_json_contents(report_file)
    attr = attr_from_report(report)
    attr["dataset_id"] = dataset_id

    # Store SmudgeplotMetrics
    ads = client.ads
    (gsm,) = ads.upsert(tbl_name, [ads.data_object_factory(tbl_name, attributes=attr)])

    # Store smudgeplot images and histogram data
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
    Extracts the attributes for the smudgeplot_metrics columns from the JSON
    report.
    """
    return {
        "haploid_coverage": report["haploid_coverage"],
        "error_fraction": report["error_fraction"],
        "top_smudges": report["top_smudges"],
        "results": report,
    }


def new_smudgeplot_run(
    rdir: Path,
    *,
    smudgeplot_cmd=None,
    threshold=None,
    min_coverage=None,
    max_coverage=None,
):
    cli_opts = {
        "-cov_min": min_coverage,
        "-cov_max": max_coverage,
    }
    params = {k: v for k, v in cli_opts.items() if v is not None}

    kmer_root = f"{SMUDGE_ROOT}.kmerpairs"
    smu_glob = f"{kmer_root}.smu"
    if not (smu_file := find_file(rdir, smu_glob)):
        run_hetmers(kmer_root, rdir, threshold, smudgeplot_cmd)
        smu_file = find_file_or_raise(rdir, smu_glob)

    return run_smudgeplot(params, rdir, smu_file, smudgeplot_cmd)


def find_report_file(rdir: Path):
    return find_file(rdir, "*smudgeplot_report.json")


def run_hetmers(kmer_root: str, rdir: Path, threshold: int, smudgeplot_cmd):
    binary, *other_args = shlex.split(smudgeplot_cmd)
    cmd_line = [
        binary,
        "hetmers",
        "-t",
        "4",
        "-L",
        str(threshold),
        "-o",
        kmer_root,
        *other_args,
    ]
    ktab_file = find_file_or_raise(rdir, "fastk.ktab").relative_to(rdir)
    with tempfile.TemporaryDirectory() as tmp_dir:
        cmd_line.extend(["-tmp", tmp_dir, str(ktab_file)])
        logging.info(f"Running: {shlex.join(cmd_line)}")
        logging.info(f"Running: {cmd_line!r}")
        run_smudgeplot_process(rdir, cmd_line)


def run_smudgeplot(params, rdir, smu_file, smudgeplot_cmd):
    cmd_line = build_smudgeplot_cmd_line(params, smu_file, smudgeplot_cmd)
    run_smudgeplot_process(rdir, cmd_line)
    return find_report_file(rdir)


def run_smudgeplot_process(rdir: Path, cmd_line: list[str]):
    try:
        subprocess.run(cmd_line, check=True, capture_output=True, cwd=rdir)  # noqa: S603
    except subprocess.CalledProcessError as cpe:
        sh_cmd = shlex.join(cmd_line)
        msg = (
            f"Error running {sh_cmd!r}; exit({cpe.returncode}):\n" + cpe.stderr.decode()
        )
        raise SmudgeplotError(msg) from None


def build_smudgeplot_cmd_line(params, smu_file, smudgeplot_cmd="smudgeplot"):
    params["--json_report"] = True
    params["-o"] = SMUDGE_ROOT

    binary, *other_args = shlex.split(smudgeplot_cmd)
    cmd_line = [binary, "all"]
    for prm, val in params.items():
        cmd_line.append(prm)
        if val is not True:
            cmd_line.append(str(val))
    cmd_line.extend(other_args)
    cmd_line.append(str(smu_file))

    return cmd_line
