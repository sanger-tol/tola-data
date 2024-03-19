from tol.api_client2 import create_api_datasource
from tol.core import core_data_object


def tolqc_ads(tolqc_url=None, api_token=None):
    tolqc = create_api_datasource(
        api_url=tolqc_url + "/api/v1",
        token=api_token,
        data_prefix="/data",
    )
    tolqc.page_size = 200
    core_data_object(tolqc)
    return tolqc
