import json
import os
import requests


class TolClient:
    def __init__(self, tolqc_url=None):
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

    def json_post(self, path, data):
        r = requests.post(f"{self.tolqc_url}/path", data=data)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            r.raise_for_status()
