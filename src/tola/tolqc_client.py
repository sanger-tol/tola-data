import json
import logging
import os
import requests

import click

from tola.db_connection import get_connection_params_entry

tolqc_alias = click.option(
    "--tolqc-alias",
    help="Name of system to connect to in ~/.connection_params.json",
    default="tolqc-production",
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


def get_url_and_alias_params(tolqc_alias, tolqc_url, api_token):
    if tolqc_url and api_token:
        return tolqc_url, api_token

    conf = get_connection_params_entry(tolqc_alias)
    if not tolqc_url:
        tolqc_url = conf.get("api_url")
    if not api_token:
        api_token = conf.get("api_token")

    return tolqc_url, api_token


class TolClient:
    def __init__(self, tolqc_url=None, api_token=None):
        self.api_path = os.getenv("API_PATH", "/api/v1").strip("/")
        self.tolqc_url = self._get_cfg_or_raise("TOLQC_URL", tolqc_url).rstrip("/")
        self.api_token = self._get_cfg_or_raise("TOLQC_API_TOKEN", api_token)

    def _get_cfg_or_raise(self, env_var, val):
        if not val:
            val = os.getenv(env_var)
        if not val:
            lc_var = env_var.lower()
            msg = (
                f"Missing '{lc_var}' argument to constructor"
                f" and environment variable '{env_var}' not set"
            )
            raise ValueError(msg)
        return val

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
