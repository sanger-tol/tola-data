import datetime as dt
import logging
import sys
from collections.abc import Callable, Iterable
from functools import cached_property
from pathlib import Path
from typing import Any

import click
from tol.core.data_object import DataObject

from tola import click_options
from tola.ndjson import DEFAULT_TZ, ndjson_row
from tola.pretty import bold
from tola.tolqc_client import TolClient
from tola.tqc.engine import dicts_to_core_data_objects, id_iterator

log = logging.getLogger(__name__)


@click.command
@click.pass_context
@click.option(
    "--public-to-downloaded",
    flag_value=True,
    help="""
      Set assemblies with "ENA Public" status to "Downloaded" using the
      timestamp of the expected assembly file on the filesystem for the
      status time where the file is present.  By default queries the database
      for all "ENA Public" assemblies, or acts on a list of "assembly.id"
      provided in the input files or ID_LIST.
    """,
)
@click.option(
    "--update-downloaded-time",
    flag_value=True,
    help="""
      Set the status time of "Downloaded" assemblies to the timestamp of the
      assembly file on the filesystem.  Acts on the "assembly.id" provided in
      the input files or ID_LIST.
    """,
)
@click_options.file
@click_options.file_format
@click_options.id_list
@click_options.pretty
def assembly(
    ctx,
    public_to_downloaded: bool,
    update_downloaded_time: bool,
    file_list: list[Path],
    file_format: str,
    id_list: list[str],
    pretty: bool,
) -> None:
    """
    Create and update assemblies.
    """
    client: TolClient = ctx.obj

    if public_to_downloaded or update_downloaded_time:
        asm_ids = tuple(
            id_iterator(
                "assembly.id",
                id_list=id_list,
                file_list=file_list,
                file_format=file_format,
            )
        )
        asm_updtr = AssemblyUpdater(client)
        if public_to_downloaded:
            if asm_ids:
                asm_updtr.update_to_downloaded(asm_ids)
            else:
                asm_updtr.update_ena_public_to_downloaded()
        else:
            asm_updtr.update_downloaded_status_time(asm_ids)

        if counts := asm_updtr.update_counts:
            on_tty = sys.stdout.isatty()
            if pretty or on_tty:
                sys.stdout.write(pretty_report_from_dict(counts, on_tty))
            else:
                print(ndjson_row(counts))


def pretty_report_from_dict(rprt: dict[str, int], on_tty: bool):
    txt = ""
    for name, n in rprt.items():
        title = name.title().replace("_", " ")
        s = "" if n == 1 else "es" if name.endswith("s") else "s"
        pad = " " * (6 - len(str(n)))
        txt += f"{pad}{bold(n) if on_tty else n}  {title}{s}\n"
    return txt


class AssemblyUpdaterError(Exception):
    """
    Error from `AssemblyUpdater` object
    """


class AssemblyFileError(Exception):
    """
    Error with assembly file
    """


class AssemblyUpdater:
    def __init__(self, client: TolClient):
        self.client = client
        self.asembly_updates = []
        self.status_updates = []
        self.new_statuses = []
        self.update_counts = {}

    @cached_property
    def location_root(self) -> Path:
        loc_id = "location.root"
        (loc,) = self.client.ads_ro.get_by_ids("metadata", [loc_id])
        if not loc:
            msg = f"No such metadata entry: {loc_id!r}"
            raise AssemblyUpdaterError(msg)
        loc_root = loc.string_value
        if loc_root is None:
            msg = f"Missing metadata.string_value column in row: {loc_id!r}"
            raise AssemblyUpdaterError(msg)
        return Path(loc_root)

    @cached_property
    def category_paths(self) -> dict[str, str]:
        cat_paths = {}
        for cat in self.client.ads_ro.get_list("assembly_category"):
            cat_paths[cat.id] = cat.hierarchy_name
        return cat_paths

    def add_counts(self, key: str, n: int) -> None:
        if n:
            counts = self.update_counts
            counts[key] = counts.get(key, 0) + n

    def should_flush_updates(self) -> bool:
        page_size = self.client.page_size
        for queue in self.asembly_updates, self.status_updates, self.new_statuses:
            if len(queue) >= page_size:
                return True
        return False

    def flush_updates(self) -> None:
        self.do_updates("assembly", self.asembly_updates)
        self.assembly_updates = []
        self.do_updates("assembly_status", self.status_updates)
        self.status_updates = []
        self.store_new_statuses()
        self.new_statuses = []

    def do_updates(self, table: str, updates: list[dict[str, Any]]) -> None:
        client = self.client
        ads = client.ads
        n = 0
        for book in client.pages(updates):
            cdo_list = dicts_to_core_data_objects(ads, table, book)
            n += (
                len([x for x in upsrtd if isinstance(x, DataObject)])
                if (upsrtd := ads.upsert(table, cdo_list))
                else 0
            )
        if n != len(updates):
            msg = (
                f"Error updating {table} table:"
                f" sent {len(updates)} but received {n} from server"
            )
            raise AssemblyUpdaterError(msg)

        self.add_counts(f"{table}_update", n)

    def store_new_statuses(self) -> None:
        client = self.client
        stored_status = client.ndjson_post(
            "loader/status/assembly", (ndjson_row(x) for x in self.new_statuses)
        )
        n = sum(len(x) for x in stored_status.values())
        if n != len(self.new_statuses):
            n_new = len(self.new_statuses)
            es = "" if n_new == 1 else "es"
            msg = (
                "Error storing new assembly_status entries:"
                f" sent {n_new} new status{es} but received {n} from server"
            )
            raise AssemblyUpdaterError(msg)

        self.add_counts("new_assembly_status", n)

    def fetch_assembly_data(self, *, filter_spec: dict[str, dict[str, Any]]):
        yield from self.client.ads_get_dict_list(
            "assembly",
            filter_spec=filter_spec,
            requested_fields=["specimen.location", "status"],
        )

    def update_ena_public_to_downloaded(self) -> None:
        filt = {
            "status.status_type.id": {
                "eq": {"value": "ENA Public"},
            }
        }
        self.run_updates(filt, self.set_downloaded)

    def update_to_downloaded(self, asm_ids: Iterable[str]) -> None:
        for book in self.client.pages(asm_ids):
            filt = {"id": {"in_list": {"value": book}}}
            self.run_updates(filt, self.set_downloaded)

    def update_downloaded_status_time(self, asm_ids: Iterable[str]) -> None:
        for book in self.client.pages(asm_ids):
            filt = {"id": {"in_list": {"value": book}}}
            self.run_updates(filt, self.set_downloaded_time)

    def run_updates(
        self,
        filter_spec: dict[str, dict[str, Any]],
        update_func: Callable[[dict[str, Any]], None],
    ) -> None:
        for asm in self.fetch_assembly_data(filter_spec=filter_spec):
            update_func(asm)
            if self.should_flush_updates():
                self.flush_updates()
        self.flush_updates()

    def build_full_file_path(self, asm: dict[str, Any]) -> Path:
        file_path = asm["file_path"]
        locn = asm["specimen.location.path"]
        cat_path = (
            self.category_paths.get(cat_name)
            if (cat_name := asm["category.id"])
            else None
        )
        if not (file_path and locn and cat_path):
            missing = []
            if not file_path:
                missing.append("assembly.file_path")
            if not locn:
                missing.append("specimen.location.path")
            if not cat_path:
                missing.append("category.hierarchy_name")
            msg = (
                f"Missing path elements for assembly.id {asm['assembly.id']!r}: "
                + ", ".join(missing)
            )
            raise AssemblyFileError(msg)
        return self.location_root / locn / cat_path / file_path

    def get_downloaded_assembly_file_modified_datetime(
        self, asm: dict[str, Any]
    ) -> dt.datetime | None:

        asm_id = asm["assembly.id"]
        full_path = self.build_full_file_path(asm)
        try:
            asm_file_stat = full_path.stat()
        except FileNotFoundError:
            msg = f"No download file for assembly.id {asm_id!r} at: {full_path}"
            raise AssemblyFileError(msg) from None

        return dt.datetime.fromtimestamp(asm_file_stat.st_mtime, tz=DEFAULT_TZ)

    def set_downloaded_time(self, asm: dict[str, Any]) -> None:
        if asm["status.status_type.id"] != "Downloaded":
            asm_id = asm["assembly.id"]
            status = asm["status.status_type.id"]
            msg = (
                f"Current status of assembly.id {asm_id!r}"
                f" is {status!r} not 'Downloaded'"
            )
            raise AssemblyUpdaterError(msg)

        try:
            mtime = self.get_downloaded_assembly_file_modified_datetime(asm)
        except AssemblyFileError as afe:
            log.warning("\n".join(str(x) for x in afe.args))
            return

        if asm["status.status_time"] != mtime:
            self.status_updates.append(
                {
                    "assembly_status.id": asm["status.id"],
                    "status_time": mtime,
                }
            )

    def set_downloaded(self, asm: dict[str, Any]) -> None:
        asm_id = asm["assembly.id"]
        try:
            mtime = self.get_downloaded_assembly_file_modified_datetime(asm)
        except AssemblyFileError as afe:
            log.info("\n".join(str(x) for x in afe.args))
            return

        if asm["status.status_type.id"] == "Downloaded":
            if asm["status.status_time"] != mtime:
                self.status_updates.append(
                    {
                        "assembly_status.id": asm["status.id"],
                        "status_time": mtime,
                    }
                )
        else:
            self.new_statuses.append(
                {
                    "assembly.id": asm_id,
                    "status_type.id": "Downloaded",
                    "status_time": mtime,
                }
            )
