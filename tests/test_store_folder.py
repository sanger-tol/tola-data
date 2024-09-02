import json
import logging
import pathlib

import pytest

from tola.store_folder import FilePatternSet, FolderLocation, upload_files
from tola.tolqc_client import TolClient


@pytest.fixture
def client():
    return TolClient(tolqc_alias="tolqc-flask")


@pytest.fixture
def data_dir():
    return pathlib.Path(__file__).parent / "data"


@pytest.fixture
def pacbio_data_dir(data_dir):
    return data_dir / "pacbio_data_dir"


def test_scan_files(data_dir, pacbio_data_dir):
    config = json.load((data_dir / "pacbio_data_report_config.json").open())
    fp_set = FilePatternSet(config=config)
    assert fp_set

    with pytest.raises(ValueError, match=r"Name for 'specimen' is missing"):
        fp_set.scan_files(pacbio_data_dir, {"x": "mBalPhy2"})

    found = fp_set.scan_files(pacbio_data_dir, {"specimen": "mBalPhy2"})
    logging.debug(found)
    assert found
    assert found["files_total_bytes"] == 38  # Each test file is 2 bytes


def test_folder_location(client):
    fl = client.get_folder_location("pacbio_data_s3")
    assert isinstance(fl, FolderLocation)
    assert isinstance(fl.pattern_set, FilePatternSet)


def test_files_upload(client, pacbio_data_dir):
    data_fldr = upload_files(
        client,
        folder_location_id="pacbio_data_s3",
        table="data",
        spec={
            "data.id": "m84098_240508_102324_s2#2093",
            "directory": pacbio_data_dir,
            "specimen": "mBalPhy2",
        },
    )
    pacbio_fldr = upload_files(
        client,
        folder_location_id="pacbio_data_s3",
        table="pacbio_run_metrics",
        spec={
            "pacbio_run_metrics.id": "m84098_240508_102324_s2",
            "directory": pacbio_data_dir,
            "specimen": "mBalPhy2",
        },
    )
    assert data_fldr
    assert pacbio_fldr
