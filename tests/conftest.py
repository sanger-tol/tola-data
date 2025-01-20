import re

import pytest

from tola.db_connection import ConnectionParamsError
from tola.tolqc_client import TolClient

skip_reason = None
skip_no_client = pytest.mark.skipif(skip_reason, reason=str(skip_reason))


@pytest.fixture(scope="session")
def client():
    """
    Return a TolClient. The "session" scope to the `pytest.fixture` ensures it
    only runs once, so that the `~/.connection_params.json` file is only
    sourced once.
    """

    try:
        client = TolClient(tolqc_alias="tolqc-test")
    except ConnectionParamsError as e:
        (reason,) = e.args
        pytest.skip(reason)

    # Check that we're testing against localhost URL
    if not re.match(r"http://(127\.0\.0\.1|::1|localhost):\d+$", client.tolqc_url):
        pytest.skip(
            f"ToLQC URL '{client.tolqc_url}' does not appear to be a test server"
        )

    # Check that we have an API token, required for tests which write to the
    # server
    if not client.api_token:
        pytest.skip("No api_token in 'tolqc-test' config")

    return client


@pytest.fixture(scope="session")
def ads(client):
    return client.ads
