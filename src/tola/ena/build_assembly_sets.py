import json
import logging
import re

import click
from tol.core.data_object import DataObject

from tola import click_options
from tola.tolqc_client import TolClient

log = logging.getLogger(__name__)


@click.command
@click_options.tolqc_alias
@click.argument("specimens", nargs=-1)
def cli(tolqc_alias, specimens):
    """
    Creates sets for any "ENA Public" assemblies which aren't in a set.
    """

    client = TolClient(
        tolqc_alias=tolqc_alias,
        page_size=1000,
    )

    build_assembly_sets(client, specimens)


ENA_PUBLIC = {"status_history.status_type.id": {"eq": {"value": "ENA Public"}}}


class AssemblySetError(Exception):
    """
    Missing or unexpected data in assemblies and assembly sets.
    """


def build_assembly_sets(client: TolClient, specimens: list[str] | None = None):
    if not specimens:
        specimens = list_specimens_with_incomplete_assembly_sets(client)
    for chapter in client.pages(specimens):
        specimen_asm: dict[str, list[DataObject]] = {}
        for asm in client.ads_get_list(
            "assembly",
            filter_spec={
                **ENA_PUBLIC,
                "specimen.id": {"in_list": {"value": chapter}},
            },
            requested_fields=[
                "name",
                "description",
                "is_reference",
                "level",
                "source_assn.source",
                "is_principal",
                "source_assn.source.name",
                "source_assn.source.description",
            ],
        ):
            if spmn := asm.specimen:
                specimen_asm.setdefault(spmn.id, []).append(asm)
            else:
                msg = (
                    f"No Specimen attached to Assembly {asm.id}"
                    " when selection on specimen names was part of the query"
                )
                raise AssemblySetError(msg)
        for spmn, assemblies in specimen_asm.items():
            update_or_create_assembly_set(client, spmn, assemblies)


def list_specimens_with_incomplete_assembly_sets(client: TolClient) -> list[str]:
    specimens = set()
    for asm in client.ads_get_list(
        "assembly",
        filter_spec={
            **ENA_PUBLIC,
            "source_assn.source.id": {"exists": {"negate": True}},
            "specimen.id": {"exists": {}},
        },
        requested_fields=["id"],
    ):
        if spcmn := asm.specimen:
            specimens.add(spcmn.id)
        else:
            msg = (
                f"No Specimen attached to Assembly {asm.id}"
                " when it was filtered on not NULL in the query"
            )
            raise AssemblySetError(msg)

    return sorted(specimens)


def update_or_create_assembly_set(
    client: TolClient, specimen: str, assemblies: list[DataObject]
):

    # Group assemblies by version
    version_sets: dict[int, list[DataObject]] = {}
    for asm in assemblies:
        m = re.search(r"^\S+\.(\d+)", asm.name or "")
        if not m:
            log.warning(
                f"Failed to parse version from {specimen!r} assembly name {asm.name!r}"
            )
            return
        i = int(m.group(1))
        version_sets.setdefault(i, []).append(asm)

    v = max(version_sets)
    for i, asm_list in sorted(version_sets.items()):
        out = []
        asm_set = None
        for asm in asm_list:
            if assn := asm.source_assn:
                for src in [x.source for x in assn]:
                    if (cmpt := src.component_type) and cmpt.id == "set":
                        if asm_set:
                            if src.id != asm_set.id:
                                msg = (
                                    f"More than one assembly set,"
                                    f" {asm_set.id}:{asm_set.name}"
                                    f" and {src.id}:{src.name}),"
                                    f" for {specimen}.{i} assembly"
                                )
                                raise AssemblySetError(msg)
                        else:
                            asm_set = src
            info = {
                "id": asm.id,
                "name": asm.name,
                "desc": asm.description,
                "category": cmpt.id if (cmpt := asm.category) else None,
                "is_principal": asm.is_principal,
                "is_reference": asm.is_reference,
                "level": asm.level,
                "set": asm_set.name if asm_set else None,
            }
            out.append(info)
        print(json.dumps(out, indent=2))
