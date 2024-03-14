import os
import requests

import click


tolqc_url = click.option(
    "--tolqc-url",
    envvar="TOLQC_URL",
    help="URL of ToL QC database if TOLQC_URL environment variable is not set",
    required=True,
)

api_token = click.option(
    "--api-token",
    envvar="API_TOKEN",
    help="API token for ToL QC if API_TOKEN environment variable is not set",
    required=True,
)


class TolClient:
    def __init__(self, tolqc_url=None, api_token=None):
        self.api_path = os.getenv(
            "TOLQC_API_PATH", os.getenv("API_PATH", "/api/v1")
        ).lstrip("/")
        self.tolqc_url = self._get_cfg_or_raise("TOLQC_URL", tolqc_url)
        self.api_token = self._get_cfg_or_raise("API_TOKEN", api_token)

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
        return "/".join((self.tolqc_url, self.api_path, path))

    def json_get(self, path, payload):
        r = requests.get(
            self._build_path(path),
            headers=self._headers(),
            params=payload,
        )
        return self._check_response(r)

    def json_post(self, path, data):
        r = requests.post(
            self._build_path(path),
            headers=self._headers(),
            data=data,
        )
        return self._check_response(r)

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
        return sorted(project_lims_ids, reverse=True)
