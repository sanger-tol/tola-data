import json
import logging

import pytest

from tola.ndjson import ndjson_row
from tola.tqc.dataset import find_dataset_file, latest_dataset
from tola.tqc.genomescope import attr_from_report, report_json_contents
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
    runner, tmp_path = fofn_runner
    ds_file = find_dataset_file(tmp_path)
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
        str(gs_results_dir(tmp_path)),
    )
    result = runner.invoke(cli, args)
    assert result.exit_code == 0
    logging.info(result.stdout)
    out = json.loads(result.stdout)
    assert len(out["image_file_list"]) == 4
    assert len(out["other_file_list"]) == 1
