import sys
from functools import cached_property
from pathlib import Path
from typing import Any

from src.tola.tolqc_client import TolClient


class AssemblyUpdaterError(Exception):
    """
    Error from `AssemblyUpdater` object
    """


class AssemblyUpdater:
    def __init__(self, client: TolClient):
        self.client = client

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

    def fetch_assembly_data(self, *, filter_spec: dict[str, dict[str, Any]]):
        yield from self.client.ads_get_dict_list(
            "assembly",
            filter_spec=filter_spec,
            requested_fields=["specimen.location"],
        )

    def update_ena_public_to_downloaded(self):
        filt = {
            "status.status_type.id": {
                "eq": {"value": "ENA Public"},
            }
        }
        for asm in self.fetch_assembly_data(filter_spec=filt):
            self.set_downloaded(asm)

    def set_downloaded(self, asm: dict[str, Any]) -> None:
        file_path = asm.get("file_path")
        locn = asm.get("specimen.location.path")
        cat_path = (
            self.category_paths.get(cat_name)
            if (cat_name := asm.get("category.id"))
            else None
        )
        if not (file_path and locn and cat_path):
            return
        full_path = self.location_root / locn / cat_path / file_path
        print(full_path, file=sys.stderr)
