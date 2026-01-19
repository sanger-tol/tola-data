import json
import re
import shlex
import subprocess
import sys
from pathlib import Path

import click
from tol.core import DataSourceFilter

from tola.ndjson import ndjson_row
from tola.store_folder import upload_files
from tola.terminal import colour_pager, pretty_dict_itr
from tola.tolqc_client import TolClient
from tola.tqc.dataset import latest_dataset_id
from tola.tqc.engine import core_data_object_to_dict


class GenomescopeError(Exception):
    """Failure to run genomescope or storing a genomescope result"""


@click.command()
@click.pass_context
@click.option(
    "--run/--upload",
    "run_flag",
    default=True,
    show_default=True,
    help=(
        """
        Whether to run genomescope in each directory in INPUT_DIRS or upload
        existing genomescope results to the ToLQC database.
        """
    ),
)
@click.option(
    "--dataset-id",
    "cli_dataset_id",
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
    "--genomescope-cmd",
    default="genomescope.R",
    show_default=True,
    help=(
        """
        Command for running genomescope.
        """
    ),
)
@click.option(
    "--lambda",
    "initial_kmer_coverage",
    type=int,
    help=(
        """
        Genomescope parameter for initial k-mer coverage.
        """
    ),
)
@click.option(
    "--ploidy",
    type=int,
    help=(
        """
        Genomescope parameter for which ploidy model to use.  The default is
        to use the value from the `specimen.ploidy` column in ToLQC, or 2 if
        that is null.
        """
    ),
)
@click.option(
    "--max-kmer-coverage",
    type=int,
    help=(
        """
        Genomescope parameter for maximum k-mer coverage.
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
    run_flag,
    cli_dataset_id,
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
    client: TolClient = ctx.obj

    if cli_dataset_id and len(input_dirs) != 1:
        sys.exit(
            f"dataset.id set to '{cli_dataset_id}' but {len(input_dirs)} INPUT_DIRS"
            " given.  Can only specify one input directory if a --dataset-id argument"
            " is set."
        )

    if (initial_kmer_coverage or ploidy or max_kmer_coverage) and not run_flag:
        sys.exit(
            "The --lambda, --ploidy or --max-kmer-coverage options are only used when"
            " the --run flag is set."
        )

    if rerun_if_no_json and run_flag:
        sys.exit("Cannot set --rerun-if-no-json with the --run flag.")

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
                report_file = new_genomescope_run(
                    rdir,
                    genomescope_cmd=genomescope_cmd,
                    initial_kmer_coverage=initial_kmer_coverage,
                    ploidy=ploidy or fetch_specimen_ploidy(client, dataset_id),
                    max_kmer_coverage=max_kmer_coverage,
                )
            else:
                report_file = find_report_file(rdir)

            if not report_file and rerun_if_no_json:
                params = genomescope_params_from_previous_run(rdir)
                report_file = run_genomescope(params, rdir, genomescope_cmd)

            if not report_file:
                msg = f"Missing report.json file in directory '{rdir}'"
                raise GenomescopeError(msg)

            rslt = store_genomescope_results(
                client,
                rdir,
                dataset_id,
                report_file,
                folder_location_id=folder_location_id,
            )

        except GenomescopeError as gse:
            (msg,) = gse.args
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


def fetch_specimen_ploidy(client, dataset_id):
    specimen_list = list(
        client.ads.get_list(
            "specimen",
            object_filters=DataSourceFilter(
                exact={"samples.data.dataset_assn.dataset.id": dataset_id}
            ),
        )
    )
    if len(specimen_list) == 1:
        ploidy = specimen_list[0].ploidy
        return None if ploidy is None else int(ploidy)

    msg = "Fetching ploidy for dataset.id {dataset_id!r}"
    if specimen_list:
        found_ids = [s.id for s in specimen_list]
        msg += f" found multiple specimens: {found_ids!r}"
    else:
        msg += " failed to find a specimen"
    raise GenomescopeError(msg)


def store_genomescope_results(
    client: TolClient,
    rdir: Path,
    dataset_id: str,
    report_file: Path,
    *,
    folder_location_id="genomescope_s3",
):
    tbl_name = "genomescope_metrics"

    ### Test for matching `report_file` in ToLQC ###

    report = file_json_contents(report_file)
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


def file_json_contents(file: Path):
    return json.loads(file.read_text())


def latest_dataset_id_or_raise(rdir):
    dataset_id = latest_dataset_id(rdir)
    if not dataset_id:
        msg = (
            "Failed to find dataset_id from a 'datasets.ndjson'"
            f" file in or above directory '{rdir}'"
        )
        raise GenomescopeError(msg)
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


def new_genomescope_run(
    rdir: Path,
    *,
    genomescope_cmd=None,
    initial_kmer_coverage=None,
    ploidy=None,
    max_kmer_coverage=None,
):
    cli_opts = {
        "--lambda": initial_kmer_coverage,
        "--ploidy": ploidy,
        "--max_kmercov": max_kmer_coverage,
    }
    params = {k: v for k, v in cli_opts.items() if v is not None}

    return run_genomescope(params, rdir, genomescope_cmd)


def find_report_file(rdir: Path):
    return find_file(rdir, "*report.json")


def genomescope_params_from_previous_run(rdir):
    pattern = "*summary.txt"
    summary_file = find_file(rdir, pattern)
    if not summary_file:
        msg = f"No '{pattern}' file in directory '{rdir}'"
        raise GenomescopeError(msg)
    return parse_summary_txt(summary_file.read_text())


def run_genomescope(params, rdir, genomescope_cmd):
    cmd_line = build_genomescope_cmd_line(params, rdir, genomescope_cmd)
    try:
        subprocess.run(cmd_line, check=True, capture_output=True, cwd=rdir)  # noqa: S603
    except subprocess.CalledProcessError as cpe:
        msg = (
            f"Error running {cmd_line} exit({cpe.returncode}):\n" + cpe.stderr.decode()
        )
        raise GenomescopeError(msg) from None
    return find_report_file(rdir)


def build_genomescope_cmd_line(params, rdir, genomescope_cmd="genomescope.R"):
    params.setdefault("--ploidy", 2)
    params.setdefault("--kmer_length", 31)
    params.setdefault("--name_prefix", "fastk_genomescope")
    params["--input"] = find_hist_txt_or_raise(rdir).relative_to(rdir)
    params["--output"] = Path(".")
    params["--json_report"] = True

    cmd_line = shlex.split(genomescope_cmd)
    for prm, val in params.items():
        cmd_line.append(prm)
        if val is not True:
            cmd_line.append(str(val))

    return cmd_line


def find_hist_txt_or_raise(rdir):
    hist_file_pattern = "*.hist.txt"
    if hist_file := find_file(rdir, hist_file_pattern):
        return hist_file

    msg = f"Failed to find file matching '{hist_file_pattern}' in '{rdir}'"
    raise GenomescopeError(msg)


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
            raise GenomescopeError(msg)
        else:
            found = fn
    return found
