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
    help="Name of connection parameters alias in ~/.connection_params.json file.",
    default="tolqc",
    show_default=True,
)

tolqc_url = click.option(
    "--tolqc-url",
    envvar="TOLQC_URL",
    help=(
        "URL of ToL QC API. Overrides TOLQC_URL environment variable if set,"
        " which in turn overrides the ~/.connection_params.json value of"
        " `api_url` for the alias."
    ),
)

api_token = click.option(
    "--api-token",
    envvar="TOLQC_API_KEY",
    help=(
        "Token for the ToL QC API. Overrides TOLQC_API_KEY environment variable"
        " if set, which in turn overrides the ~/.connection_params.json value"
        " of `api_token` for the alias."
    ),
)

file_format = click.option(
    "--format",
    "file_format",
    type=click.Choice(
        ["NDJSON", "TXT"],
        case_sensitive=False,
    ),
    default=None,
    show_default=True,
    help="Format of input file(s) or STDIN",
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
    def __init__(
        self, tolqc_url=None, api_token=None, tolqc_alias="tolqc", page_size=200
    ):
        self.api_path = os.getenv("TOLQC_API_PATH", "/api/v1").strip("/")
        self.page_size = page_size

        if conf := get_connection_params_entry(
            tolqc_alias, no_params_file_ok=(tolqc_alias == "tolqc")
        ):
            self._tolqc_url = tolqc_url or conf.get("api_url")
            self.api_token = api_token or conf.get("api_token")
            self._set_proxy(conf)
        else:
            # Set default URL if there is no config file
            self._tolqc_url = tolqc_url or "https://qc.tol.sanger.ac.uk"
            self.api_token = api_token

    @cached_property
    def tolqc_url(self):
        return self._tolqc_url.rstrip("/")

    @cached_property
    def ads(self):
        tolqc = create_api_datasource(
            api_url="/".join((self.tolqc_url, self.api_path)),
            token=self.api_token,
            data_prefix="/data",
        )
        tolqc.page_size = self.page_size
        core_data_object(tolqc)
        return tolqc

    def _set_proxy(self, conf):
        if proxy := conf.get("proxy"):
            scheme = (
                "HTTPS_PROXY" if self.tolqc_url.startswith("https") else "HTTP_PROXY"
            )
            os.environ[scheme] = proxy

    @cached_property
    def token_header(self):
        if tkn := self.api_token:
            return {"Token": tkn}
        return {}

    def build_path(self, path):
        if path.startswith("/"):
            msg = "Error: unnecessary leading '/' in path: {path!r}"
            raise ValueError(msg)
        return "/".join((self.tolqc_url, self.api_path, path))

    def json_get(self, path, payload=None):
        enc = self._encode_payload(payload)
        r = requests.get(
            self.build_path(path),
            params=enc,
            timeout=120,
        )
        logging.debug(f"URL = {r.url}")
        return self._check_response(r)

    def json_post(self, path, data):
        r = requests.post(
            self.build_path(path),
            headers=self.token_header,
            data=data,
            timeout=120,
        )
        return self._check_response(r)

    def _encode_payload(self, payload):
        enc = {}
        if not payload:
            return enc
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

    def pages(self, book):
        page = self.page_size
        for i in range(0, len(book), page):
            yield book[i : i + page]

    def list_project_study_ids(self):
        rspns_json = self.json_get("data/project")
        project_study_ids = []
        for proj in rspns_json["data"]:
            if study_id := proj["attributes"].get("study_id"):
                project_study_ids.append(study_id)
        return sorted(project_study_ids)
