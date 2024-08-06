import json
import logging
import pathlib
import pytest
from tola.store_folder import FilePatternSet


def test_scan_files():
    data_dir = pathlib.Path(__file__).parent / "data"
    config = json.load((data_dir / "pacbio_data_report_config.json").open())
    fp_set = FilePatternSet(config=config)
    assert fp_set

    with pytest.raises(ValueError, match=r"Name for 'specimen' is missing"):
        fp_set.scan_files(data_dir / "pacbio_data_dir", {"x": "mBalPhy2"})

    found = fp_set.scan_files(data_dir / "pacbio_data_dir", {"specimen": "mBalPhy2"})
    assert found["files_bytes_total"] == 0  # Test files are all empty
    logging.debug(found)
    assert found
