import json
import logging
import os
import pathlib
import requests

from functools import cached_property

import click

from tol.api_client2 import create_api_datasource
from tol.core import core_data_object

from tola.db_connection import get_connection_params_entry

ca_file = pathlib.Path("/etc/ssl/certs/ca-certificates.crt")
if ca_file.exists():
    os.environ.setdefault("REQUESTS_CA_BUNDLE", str(ca_file))


tolqc_alias = click.option(
    "--tolqc-alias",
    help="Name of system to connect to in ~/.connection_params.json",
    default="tolqc",
    show_default=True,
)

tolqc_url = click.option(
    "--tolqc-url",
    envvar="TOLQC_URL",
    help="URL of ToL QC database if TOLQC_URL environment variable is not set",
)

api_token = click.option(
    "--api-token",
    envvar="TOLQC_API_TOKEN",
    help="API token for ToL QC if TOLQC_API_TOKEN environment variable is not set",
)

log_level = click.option(
    "--log-level",
    type=click.Choice(
        ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        case_sensitive=False,
    ),
    default="WARNING",
    hidden=True,
    help="Diagnostic messages to show.",
)


class TolClient:
    def __init__(self, tolqc_url=None, api_token=None, tolqc_alias=None):
        self.api_path = os.getenv("API_PATH", "/api/v1").strip("/")
        conf = get_connection_params_entry(tolqc_alias)
        self.tolqc_url = (tolqc_url or conf["api_url"]).rstrip("/")
        self.api_token = api_token or conf["api_token"]
        self._set_proxy(conf)

    @cached_property
    def ads_client(self):
        tolqc = create_api_datasource(
            api_url="/".join((self.tolqc_url, self.api_path)),
            token=self.api_token,
            data_prefix="/data",
        )
        tolqc.page_size = 200
        core_data_object(tolqc)
        return tolqc

    def _set_proxy(self, conf):
        if proxy := conf.get("proxy"):
            scheme = "HTTPS_PROXY" if self.tolqc_url.startswith("https") else "HTTP_PROXY"
            os.environ[scheme] = proxy

    def _headers(self):
        return {"Token": self.api_token}

    def _build_path(self, path):
        if path.startswith("/"):
            msg = "Error: unnecessary leading '/' in path: {path!r}"
            raise ValueError(msg)
        return "/".join((self.tolqc_url, self.api_path, path))

    def json_get(self, path, payload):
        enc = self._encode_payload(payload)
        r = requests.get(
            self._build_path(path),
            headers=self._headers(),
            params=enc,
        )
        logging.debug(f"URL = {r.url}")
        return self._check_response(r)

    def json_post(self, path, data):
        r = requests.post(
            self._build_path(path),
            headers=self._headers(),
            data=data,
        )
        return self._check_response(r)

    def _encode_payload(self, payload):
        if not payload:
            return payload
        enc = {}
        for k, v in payload.items():
            if type(v) in (dict, list):
                enc[k] = json.dumps(v, separators=(",", ":"))
            else:
                enc[k] = v
        return enc

    def _check_response(self, response):
        if response.status_code == requests.codes.ok:
            return response.json()
        else:
            response.raise_for_status()

    def list_project_lims_ids(self):
        rspns_json = self.json_get("data/project", {})
        project_lims_ids = []
        for proj in rspns_json["data"]:
            if lims_id := proj["attributes"].get("lims_id"):
                project_lims_ids.append(lims_id)
        return sorted(project_lims_ids)
