import logging

import pytest

from tola.db_connection import ConnectionParamsError, get_connection_params_entry
from tola.subtrack import SubTrack


@pytest.fixture
def subtrack_db():
    # Skip test if subtrack is not configured
    try:
        get_connection_params_entry("subtrack")
    except ConnectionParamsError as cpe:
        pytest.skip(cpe.args)

    return SubTrack()


def test_subtrack_fetch(subtrack_db):
    subtrack_db.page_size = 3
    query_files = [
        "27453_2#2.cram",
        "46307_2#2.cram",
        "48906_2#8.cram",
        "49063_1-2#1.cram",
        "m84047_240214_121625_s3.hifi_reads.bc2038.bam",
        "m84047_241204_111012_s1.hifi_reads.bc2081--bc2081.bam",
        "m84098_230922_133354_s4.hifi_reads.bc2064.bam",
        "m84098_240911_120336_s3.hifi_reads.bc2093.bam",
    ]
    sub_info = list(subtrack_db.fetch_submission_info(query_files))
    assert len(sub_info) == len(query_files)
    logging.debug(sub_info)
