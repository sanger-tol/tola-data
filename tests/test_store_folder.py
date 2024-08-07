import json
import logging
import pathlib

import pytest
from tola.store_folder import FilePatternSet, FolderLocation
from tola.tolqc_client import TolClient


def test_scan_files():
    data_dir = pathlib.Path(__file__).parent / "data"
    config = json.load((data_dir / "pacbio_data_report_config.json").open())
    fp_set = FilePatternSet(config=config)
    assert fp_set

    with pytest.raises(ValueError, match=r"Name for 'specimen' is missing"):
        fp_set.scan_files(data_dir / "pacbio_data_dir", {"x": "mBalPhy2"})

    found = fp_set.scan_files(data_dir / "pacbio_data_dir", {"specimen": "mBalPhy2"})
    logging.debug(found)
    assert found
    assert found["files_bytes_total"] == 0  # Test files are all empty


def test_folder_location():
    cl = TolClient(tolqc_alias="tolqc-flask")
    fl = cl.get_folder_location("pacbio_data_s3")
    assert isinstance(fl, FolderLocation)
    assert isinstance(fl.pattern_set, FilePatternSet)
