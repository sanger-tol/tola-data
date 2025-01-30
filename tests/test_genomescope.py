import json
import logging
from pathlib import Path
from textwrap import dedent

import pytest

from tola.ndjson import ndjson_row
from tola.tqc.dataset import find_dataset_file, latest_dataset
from tola.tqc.genomescope import (
    attr_from_report,
    build_genomescope_cmd_line,
    get_genomescope_params,
    parse_summary_txt,
    report_json_contents,
)
from tola.tqc.tqc_cmd import cli


def gs_results_dir(base):
    return base / "pacbio" / "kmer" / "k31"


@pytest.fixture(scope="session")
def gscope_fldr_loc(ads):
    fldr_cfg = {
        "uri_prefix": "s3://tolqc-dev/test/genomescope",
        "files_template": {
            "image_file_patterns": [
                {
                    "caption": "Genomescope 2.0 linear plot",
                    "pattern": ".*linear_plot\\.png",
                },
                {
                    "caption": "Genomescope 2.0 log plot",
                    "pattern": ".*log_plot\\.png",
                },
                {
                    "caption": "Genomescope 2.0 transformed linear plot",
                    "pattern": ".*transformed_linear_plot\\.png",
                },
                {
                    "caption": "Genomescope 2.0 transformed log plot",
                    "pattern": ".*transformed_log_plot\\.png",
                },
            ],
            "other_file_patterns": [
                {
                    "caption": "Kmer counts histogram data",
                    "pattern": ".*\\.hist\\.txt",
                }
            ],
        },
    }

    tbl = "folder_location"
    fldr_obj = ads.data_object_factory(
        tbl, id_="genomescope_test_s3", attributes=fldr_cfg
    )
    (fldr_loc,) = ads.upsert(tbl, [fldr_obj])
    return fldr_loc


def test_get_params(fofn_dir):
    rdir = gs_results_dir(fofn_dir)
    params = get_genomescope_params(rdir)
    assert params == {
        "--input": "tests/hugegenome.hist",
        "--kmer_length": "31",
        "--output": "test_out",
        "--ploidy": "2",
    }


def test_build_genomescope_cmd_line(fofn_runner):  # noqa: ARG001
    rdir = gs_results_dir(Path("./fofn"))
    files = list(rdir.iterdir())
    logging.info(files)
    cmd_line = build_genomescope_cmd_line(rdir)
    assert cmd_line == [
        "genomescope.R",
        "--input",
        "fofn/pacbio/kmer/k31/test.hist.txt",
        "--output",
        "fofn/pacbio/kmer/k31",
        "--ploidy",
        "2",
        "--kmer_length",
        "31",
        "--json-report",
    ]


def test_parse_summary_txt():
    txt = dedent(
        """
        GenomeScope version 2.0
        input file = A file name with spaces
        output directory = .
        p = 2
        k = 31
        name prefix = mVulVul1.k31
        NO_UNIQUE_SEQUENCE set to TRUE

        property                      min               max
        Homozygous (aa)               99.6559%          99.6636%
        Heterozygous (ab)             0.336356%         0.344076%
        Genome Haploid Length         NA bp             2,380,675,076 bp
        Genome Repeat Length          277,203,861 bp    277,376,549 bp
        Genome Unique Length          2,102,730,139 bp  2,104,040,065 bp
        Model Fit                     93.8892%          99.0823%
        Read Error Rate               0.0899951%        0.0899951%
        """
    )
    params = parse_summary_txt(txt)
    assert params == {
        "--input": "A file name with spaces",
        "--kmer_length": "31",
        "--name_prefix": "mVulVul1.k31",
        "--no_unique_sequence": True,
        "--output": ".",
        "--ploidy": "2",
    }


def test_process_report(fofn_dir):
    assert (rprt := report_json_contents(gs_results_dir(fofn_dir)))
    flat = attr_from_report(rprt)
    assert flat == {
        "kmer": 31,
        "ploidy": 2,
        "kcov_init": None,
        "homozygous": 0.998564047901143,
        "heterozygous": 0.00143595209885725,
        "haploid_length": 5878521033,
        "unique_length": 3981236382,
        "repeat_length": 1897284651,
        "kcov": 6.56564770932383,
        "model_fit": 0.992057997138315,
        "read_error_rate": 0.00269857308904897,
        "results": rprt,
    }


def test_load_genomescope(client, fofn_runner, test_alias, gscope_fldr_loc):
    here = Path("./fofn")
    ds_file = find_dataset_file(here)
    dataset = latest_dataset(ds_file)

    # Ensure dataset is loaded
    rspns = client.ndjson_post("loader/dataset", [ndjson_row(dataset)])
    dataset = rspns.get("new", rspns.get("existing"))[0]
    ds_file.write_text(ndjson_row(dataset))

    args = (
        "--tolqc-alias",
        test_alias,
        "genomescope",
        "--folder-location",
        gscope_fldr_loc.id,
        str(gs_results_dir(here)),
    )
    result = fofn_runner.invoke(cli, args)
    assert result.exit_code == 0
    out = json.loads(result.stdout)
    logging.info("\n" + json.dumps(out, indent=2))
    assert dataset["dataset.id"] == out["dataset.id"]
    assert len(out["image_file_list"]) == 4
    assert len(out["other_file_list"]) == 1
