import json
import logging

import pytest

from tola.store_folder import FilePatternSet, FolderLocation, upload_files

log = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def pacbio_data_dir(data_dir):
    return data_dir / "pacbio_data_dir"


@pytest.fixture(scope="session")
def pacbio_fldr_loc(ads):
    fldr_cfg = {
        "uri_prefix": "s3://tolqc-dev/test/pacbio_run",
        "files_template": {
            "image_file_patterns": [
                {
                    "caption": "Base yield density",
                    "pattern": "base_yield_plot\\.png",
                },
                {
                    "caption": "Barcode quality distribution",
                    "pattern": "bq_histogram\\.png",
                },
                {
                    "caption": "Read quality distribution",
                    "pattern": "ccs_accuracy_hist\\.png",
                },
                {
                    "caption": "Read length distribution (all)",
                    "pattern": "ccs_all_readlength_hist_plot\\.png",
                },
                {
                    "caption": "HiFi yield by read length",
                    "pattern": "ccs_hifi_read_length_yield_plot\\.png",
                },
                {
                    "caption": "Number of passes",
                    "pattern": "ccs_npasses_hist\\.png",
                },
                {
                    "caption": "HiFi read length distribution",
                    "pattern": "ccs_readlength_hist_plot\\.png",
                },
                {
                    "caption": "Control concordance",
                    "pattern": "concordance_plot\\.png",
                },
                {
                    "caption": "Insert read length density",
                    "pattern": "hexbin_length_plot\\.png",
                },
                {
                    "caption": "CpG methylation in reads",
                    "pattern": "m5c_detections\\.png",
                },
                {
                    "caption": "CpG methylation in reads histogram",
                    "pattern": "m5c_detections_hist\\.png",
                },
                {
                    "caption": "Number of reads per barcode",
                    "pattern": "nreads\\.png",
                },
                {
                    "caption": "Number of reads per barcode histogram",
                    "pattern": "nreads_histogram\\.png",
                },
                {
                    "caption": "Loading evaluation",
                    "pattern": "raw_read_length_plot\\.png",
                },
                {
                    "caption": "Polymerase read length",
                    "pattern": "readLenDist0\\.png",
                },
                {
                    "caption": "Control polymerase read length",
                    "pattern": "readlength_plot\\.png",
                },
                {
                    "caption": "Mean readlength histogram",
                    "pattern": "readlength_histogram\\.png",
                },
                {
                    "caption": "Accuracy versus read length density",
                    "pattern": "readlength_qv_hist2d\\.hexbin\\.png",
                },
            ]
        },
    }

    tbl = "folder_location"
    fldr_obj = ads.data_object_factory(tbl, id_="pacbio_test_s3", attributes=fldr_cfg)
    (fldr_loc,) = ads.upsert(tbl, [fldr_obj])
    return fldr_loc


def test_scan_files(data_dir, pacbio_data_dir):
    config = json.load((data_dir / "pacbio_data_report_config.json").open())
    fp_set = FilePatternSet(config=config)
    assert fp_set

    with pytest.raises(ValueError, match=r"Name for 'specimen' is missing"):
        fp_set.scan_files(pacbio_data_dir, {"x": "mBalPhy2"})

    found = fp_set.scan_files(pacbio_data_dir, {"specimen": "mBalPhy2"})
    log.debug(found)
    assert found
    assert found["files_total_bytes"] == 38  # Each test file is 2 bytes


def test_folder_location(client, pacbio_fldr_loc):  # noqa: ARG001
    fl = client.get_folder_location("pacbio_test_s3")
    assert isinstance(fl, FolderLocation)
    assert isinstance(fl.pattern_set, FilePatternSet)


def test_files_upload(client, pacbio_data_dir, pacbio_fldr_loc):  # noqa: ARG001
    upload_spec = {
        "folder_location_id": pacbio_fldr_loc.id,
        "table": "pacbio_run_metrics",
        "spec": {
            "pacbio_run_metrics.id": "m84098_240508_102324_s2",
            "directory": pacbio_data_dir,
            # "specimen": "mBalPhy2",
        },
    }
    fldr1 = upload_files(client, **upload_spec)
    fldr2 = upload_files(client, **upload_spec)
    assert fldr1 and fldr2
