import json
import re
import subprocess
import sys
from pathlib import Path

import click

from tola.ndjson import ndjson_row
from tola.store_folder import upload_files
from tola.terminal import colour_pager, pretty_dict_itr
from tola.tqc.dataset import latest_dataset_id
from tola.tqc.engine import core_data_object_to_dict


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
    "--run",
    "run_flag",
    flag_value=True,
    default=False,
    help=(
        """
        Run genomescope in each directory in INPUT_DIRS.  (The default action
        is to load existing genomescope results into the ToLQC database, not
        to run genomescope.)
        """
    ),
)
@click.option(
    "--lambda",
    "initial_kmer_coverage",
    default=None,
    type=int,
    help=(
        """
        Genomescope parameter for initial k-mer coverage.
        """
    ),
)
@click.option(
    "--ploidy",
    default=None,
    type=int,
    help=(
        """
        Genomescope parameter for which ploidy model to use.
        """
    ),
)
@click.option(
    "--max-kmer-coverage",
    default=None,
    type=int,
    help=(
        """
        Genomescope parameter for intial k-mer coverage.
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
    help="Folder location to save image and histogram data files to.",
)
@click.option(
    "--genomescope-cmd",
    default="genomescope.R",
    show_default=True,
    help=(
        """
        Command for running genomescope.
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
def genomescope(
    ctx,
    dataset_id,
    run_flag,
    initial_kmer_coverage,
    ploidy,
    max_kmer_coverage,
    rerun_if_no_json,
    folder_location_id,
    genomescope_cmd,
    input_dirs,
):
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

    if (initial_kmer_coverage or ploidy or max_kmer_coverage) and not run_flag:
        sys.exit(
            "Must specify --run if setting any of"
            " --lambda, --ploidy or --max-kmer-coverage"
        )

    if rerun_if_no_json and run_flag:
        sys.exit("Cannot set --rerun-if-no-json with the --run flag.")

    results = []
    failures = []

    for rdir in input_dirs:
        try:
            if run_flag:
                rslt = run_genomescope_and_store_results(
                    client,
                    rdir,
                    dataset_id=dataset_id,
                    folder_location_id=folder_location_id,
                    genomescope_cmd=genomescope_cmd,
                    initial_kmer_coverage=initial_kmer_coverage,
                    ploidy=ploidy,
                    max_kmer_coverage=max_kmer_coverage,
                )
            else:
                rslt = store_genomescope_results(
                    client,
                    rdir,
                    dataset_id=dataset_id,
                    rerun_if_no_json=rerun_if_no_json,
                    folder_location_id=folder_location_id,
                    genomescope_cmd=genomescope_cmd,
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


def run_genomescope_and_store_results(
    client,
    rdir: Path,
    *,
    dataset_id=None,
    folder_location_id="genomescope_s3",
    genomescope_cmd=None,
    initial_kmer_coverage=None,
    ploidy=None,
    max_kmer_coverage=None,
):
    if not dataset_id:
        latest_dataset_id_or_raise(rdir)


def store_genomescope_results(
    client,
    rdir: Path,
    *,
    dataset_id=None,
    rerun_if_no_json=False,
    folder_location_id="genomescope_s3",
    genomescope_cmd=None,
):
    tbl_name = "genomescope_metrics"

    if not dataset_id:
        latest_dataset_id_or_raise(rdir)

    report = report_json_contents(rdir, rerun_if_no_json, genomescope_cmd)
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


def latest_dataset_id_or_raise(rdir):
    dataset_id = latest_dataset_id(rdir)
    if not dataset_id:
        msg = (
            "Failed to find dataset_id from a 'datasets.ndjson'"
            f" file in or above directory '{rdir}'"
        )
        raise StoreGenomescopeError(msg)
    return dataset_id


def attr_from_report(report):
    """
    Extracts the attributes for the genomescope_metrics columns from the JSON report
    """
    param = report["input_parameters"]
    return {
        # Input parameters
        "kmer": param["kmer_length"],
        "ploidy": param["ploidy"],
        "kcov_init": param["initial_kmer_coverage"],
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


def report_json_contents(rdir: Path, rerun_if_no_json=False, genomescope_cmd=None):
    # Find report.json file
    report_file = find_file(rdir, "*report.json")
    if not report_file and rerun_if_no_json:
        report_file = rerun_genomescope(rdir, genomescope_cmd)
    if not report_file:
        msg = f"Missing report.json file in directory '{rdir}'"
        raise StoreGenomescopeError(msg)

    return json.loads(report_file.read_text())


def rerun_genomescope(rdir, genomescope_cmd):
    cmd_line = build_genomescope_cmd_line(rdir, genomescope_cmd)
    try:
        subprocess.run(cmd_line, check=True, capture_output=True)  # noqa: S603
    except subprocess.CalledProcessError as cpe:
        msg = (
            f"Error running {cmd_line} exit({cpe.returncode}):\n" + cpe.stderr.decode()
        )
        raise StoreGenomescopeError(msg) from None
    return find_file(rdir, "*report.json")


def build_genomescope_cmd_line(rdir, genomescope_cmd="genomescope.R"):
    params = get_genomescope_params(rdir)
    params["--input"] = str(find_file(rdir, "*.hist.txt"))
    params["--output"] = str(rdir)
    params["--json_report"] = True

    cmd_line = genomescope_cmd.split()
    for prm, val in params.items():
        cmd_line.append(prm)
        if val is not True:
            cmd_line.append(val)

    return cmd_line


def get_genomescope_params(rdir):
    pattern = "*summary.txt"
    summary_file = find_file(rdir, pattern)
    if not summary_file:
        msg = f"No '{pattern}' file in directory '{rdir}'"
        raise StoreGenomescopeError(msg)
    return parse_summary_txt(summary_file.read_text())


def parse_summary_txt(summary):
    cli_patterns = {
        "--input": r"^input file = (.+)",
        "--output": r"^output directory = (.+)",
        "--ploidy": r"^p = (\d+)",
        "--kmer_length": r"^k = (\d+)",
        "--name_prefix": r"^name prefix = (.+)",
        "--lambda": r"^initial kmercov estimate = (\d+)",
        "--max_kmercov": r"^max_kmercov = (\d+)",
        "--verbose": r"^VERBOSE set to (TRUE)",
        "--no_unique_sequence": r"^NO_UNIQUE_SEQUENCE set to (TRUE)",
        "--topology": r"^topology = (\d+)",
        "--initial_repetitiveness": r"^initial repetitiveness = (\S+)",
        "--initial_heterozygosities": r"^initial heterozygosities = (\S+)",
        "--transform_exp": r"^TRANSFORM_EXP = (\d+)",
        "--testing": r"^TESTING set to (TRUE)",
        "--true_params": r"^TRUE_PARAMS = (\d+)",
        "--trace_flag": r"^TRACE_FLAG set to (TRUE)",
        "--num_rounds": r"^NUM_ROUNDS = (\d+)",
        "--typical_error": r"^TYPICAL_ERROR = (\d+)",
        # Not reported in summary.txt:
        #   --json_report
        #   --fitted_hist
        #   --start_shift
    }
    params = {}
    for cli, pattern in cli_patterns.items():
        if m := re.search(pattern, summary, re.MULTILINE):
            val = m.group(1)
            params[cli] = True if val == "TRUE" else val

    return params


def find_file(rdir, pattern):
    found = None
    for fn in rdir.glob(pattern):
        if found:
            msg = f"More than one '{pattern}' in '{rdir}': '{found}' and '{fn}'"
            raise StoreGenomescopeError(msg)
        else:
            found = fn
    return found
