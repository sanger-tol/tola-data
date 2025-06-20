import json
import logging
import os
import re
from functools import cached_property
from io import StringIO
from json.decoder import JSONDecodeError
from pathlib import Path

import requests
from tol.api_client import create_api_datasource
from tol.core import DataSourceFilter, core_data_object
from tol.core.datasource_error import DataSourceError

from tola.db_connection import get_connection_params_entry
from tola.s3client import S3Client
from tola.store_folder import FolderLocation
from tola.terminal import TerminalDict
from tola.tqc.engine import core_data_object_to_dict

ca_file = Path("/etc/ssl/certs/ca-certificates.crt")
if ca_file.exists():
    os.environ.setdefault("REQUESTS_CA_BUNDLE", str(ca_file))


class TolClientError(Exception):
    """Error from TolClient"""


class TolClient:
    def __init__(
        self, tolqc_url=None, api_token=None, tolqc_alias="tolqc", page_size=200
    ):
        self.api_path = os.getenv("TOLQC_API_PATH", "/api/v1").strip("/")
        self.page_size = page_size
        self.tolqc_alias = tolqc_alias

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
            retries=False,
        )
        tolqc.page_size = self.page_size
        core_data_object(tolqc)
        return tolqc

    @cached_property
    def build_cdo(self):
        """
        Returns a function which builds a CoreDataObject (cdo)
        """
        obj_bldr = self.ads.data_object_factory

        def cdo_builder(table: str, name: str = None, attr: dict = None):
            return obj_bldr(table, id_=name, attributes=attr)

        return cdo_builder

    def fetch_or_store_one(self, table: str, spec: dict, key=None):
        """
        Fetches a CoreDataObject from a table given the flattened dict `spec`,
        or stores a new one. The optional `key` parameter is the name of the
        key to search under. Throws TolClientError exceptions if the search
        returns multiple records, or when storing a new record fails.

        Returns the CoreDataObject.
        """

        ads = self.ads
        id_key = f"{table}.id"
        if key is None or key == id_key:
            find_val = name = spec.get(id_key)
            store = {k: v for k, v in spec.items() if k != id_key}
        else:
            find_val = spec.get(key)
            name = None
            store = spec

        if find_val is None:
            msg = f"No value under key '{key or id_key}' in spec:\n{spec}"
            raise TolClientError(msg)

        # Is there an existing record?
        if exstng := list(
            ads.get_list(
                table, object_filters=DataSourceFilter(exact={key or "id": find_val})
            )
        ):
            if len(exstng) == 1:
                return exstng[0]
            else:
                msg = (
                    f"Multiple matches: found {len(exstng)}"
                    f" records in {table} where {key} = {find_val!r}:\n"
                    + "".join(
                        TerminalDict(core_data_object_to_dict(x), key=key).pretty()
                        for x in exstng
                    )
                )
                raise TolClientError(msg)

        # Create a new record
        upsrtd = list(ads.upsert(table, [self.build_cdo(table, name, store)]))
        if len(upsrtd) == 1:
            return upsrtd[0]
        else:
            msg = f"Expecting 1 new {table} record but got {len(upsrtd)}"
            if upsrtd:
                msg = (
                    msg
                    + ":\n"
                    + "".join(
                        TerminalDict(core_data_object_to_dict(x), key=key).pretty()
                        for x in upsrtd
                    )
                )
            raise TolClientError(msg)

    @cached_property
    def s3(self):
        return S3Client()

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

    def ndjson_post(self, path, ndjson_str_itr, max_request_size=2 * 1024**2):
        rspns = {}
        for chunk in self.chunk_rows(ndjson_str_itr, max_request_size):
            chunk_rspns = self.json_post(path, chunk)
            for label, array in chunk_rspns.items():
                rspns.setdefault(label, []).extend(array)
        return rspns

    def chunk_rows(self, ndjson_str_itr, max_request_size):
        chunk = StringIO()
        for ndj_str in ndjson_str_itr:
            if chunk.tell() + len(ndj_str) > max_request_size:
                yield chunk.getvalue()
                chunk.seek(0)
                chunk.truncate(0)
            chunk.write(ndj_str)

        yield chunk.getvalue()

    def download_file(self, path, filename=None):
        r = requests.get(
            self.build_path(path),
            stream=True,
            timeout=120,
        )
        r.raise_for_status()

        if not filename:
            filename = self._content_disposition_filename(r)

        # Write the response to the file
        with open(filename, "wb") as fh:
            for chunk in r.iter_content(chunk_size=8192):
                fh.write(chunk)

        return filename

    def stream_lines(self, path, payload=None):
        enc = self._encode_payload(payload)
        r = requests.get(
            self.build_path(path),
            params=enc,
            stream=True,
            timeout=120,
        )

        itr = r.iter_lines()
        try:
            first = next(itr)
            # Status is not available with stream=True until the first content
            # is fetched:
            if r.status_code != requests.codes.ok:
                try:
                    body = json.loads(first)
                    if "errors" in body:
                        err = body["errors"][0]
                        raise DataSourceError(
                            title=err.get("title"),
                            detail=err.get("detail"),
                            status_code=r.status_code,
                        )
                except JSONDecodeError:
                    pass
                r.raise_for_status()
        except StopIteration:
            # Zero lines in reply
            return
        yield first
        yield from itr

    def _content_disposition_filename(self, r):
        """Extracts the filename from the Content-Disposition header"""

        disposition_hdr = r.headers.get("content-disposition", "<MISSING>")
        if m := re.match(r'attachment; filename="([^"/]+)"$', disposition_hdr):
            return m.group(1)
        else:
            msg = (
                "Failed to extract filename from Content-Disposition"
                f" header: '{disposition_hdr}'"
            )
            raise ValueError(msg)

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

    def list_auto_sync_study_ids(self) -> list[int]:
        rspns_json = self.json_get(
            "data/study", {"filter": {"exact": {"auto_sync": True}}}
        )
        return sorted(int(x["id"]) for x in rspns_json["data"])

    @cached_property
    def sex_table(self):
        tbl = {}
        for obj in self.ads.get_list("sex"):
            sex = obj.id
            tbl[uc_munge(sex)] = sex
        return tbl

    def get_folder_location(self, folder_location_id: str) -> FolderLocation:
        return self.__folder_location_dict.get(folder_location_id)

    @cached_property
    def __folder_location_dict(self):
        rspns_json = self.json_get("data/folder_location")
        fldr_loc = {}
        for fl in rspns_json["data"]:
            fldr_loc_id = fl["id"]
            attr = fl["attributes"]
            fldr_loc[fldr_loc_id] = FolderLocation(
                fldr_loc_id,
                attr["uri_prefix"],
                attr["files_template"],
            )
        return fldr_loc


def uc_munge(txt):
    return re.sub(r"\W+", "_", txt).strip("_").upper()
