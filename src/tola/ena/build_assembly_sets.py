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
                    f"Weird! No Specimen attached to Assembly {asm.id}"
                    " when selection on specimen names was part of the query"
                )
                raise AssemblySetError(msg)
        for spmn, assemblies in specimen_asm.items():
            for asm_set in build_assembly_sets_for_specimen(client, spmn, assemblies):
                asm_set.create_or_update(client)


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
                f"Weird! No Specimen attached to Assembly {asm.id}"
                " when it was filtered on not NULL in the query"
            )
            raise AssemblySetError(msg)

    return sorted(specimens)


class AssemblySet:
    def __init__(
        self,
        *,
        source: DataObject,
        components: list[DataObject],
        version: int,
        current_version: int,
    ):
        self.source = source
        self.components = components
        self.version = version
        self.current_version = current_version

    def create_or_update(self, client):
        ads = client.ads
        set_id = self.source.id
        if not set_id:
            self.source = ads.insert("assembly", [self.source])[0]
            set_id = self.source.id
        for cmpt in self.components:
            src_assn = None
            if cmpt.source_assn:
                for assn in cmpt.source_assn:
                    if (src := assn.source) and src.id == set_id:
                        src_assn = assn
                        break
            if not src_assn:
                src_assn = ads.insert(
                    "assembly_source",
                    [
                        client.build_cdo(
                            "assembly_source",
                            None,
                            {
                                "assembly_id": cmpt.id,
                                "source_assembly_id": set_id,
                            },
                        ),
                    ],
                )


def build_assembly_sets_for_specimen(
    client: TolClient, specimen: str, assemblies: list[DataObject]
) -> list[AssemblySet]:

    # Group assemblies by version
    version_sets: dict[int, list[DataObject]] = {}
    for asm in assemblies:
        m = re.search(r"^\S+\.(\d+)", asm.name or "")
        if not m:
            msg = (
                f"Failed to parse version from {specimen!r} assembly name {asm.name!r}"
            )
            raise AssertionError(msg)
        i = int(m.group(1))
        version_sets.setdefault(i, []).append(asm)

    v = max(version_sets)
    specimen_sets = []
    for i, asm_list in sorted(version_sets.items()):
        set_asm = None
        set_name = f"{specimen}.{i}"
        cmpts = []
        for asm in asm_list:
            if assn := asm.source_assn:
                for src in [x.source for x in assn]:
                    if (cmpt := src.component_type) and cmpt.id == "set":
                        if set_asm:
                            if src.id != set_asm.id:
                                msg = (
                                    f"More than one assembly set,"
                                    f" {set_asm.id}:{set_asm.name}"
                                    f" and {src.id}:{src.name}),"
                                    f" for {set_name} assembly"
                                )
                                raise AssemblySetError(msg)
                        else:
                            set_asm = src
            cmpts.append(asm)

            # info = {
            #     "id": asm.id,
            #     "name": asm.name,
            #     "desc": asm.description,
            #     "category": cmpt.id if (cmpt := asm.category) else None,
            #     "is_principal": asm.is_principal,
            #     "is_reference": asm.is_reference,
            #     "level": asm.level,
            #     "set": set_asm.name if set_asm else None,
            # }
            # out.append(info)
        # print(json.dumps(out, indent=2))

        if not set_asm:
            name = set_name
            set_asm = client.build_cdo(
                "assembly",
                None,
                {
                    "name": name,
                    "description": f"Assembly set {name}",
                    "specimen_id": specimen,
                    "component_type_id": "set",
                },
            )
        specimen_sets.append(
            AssemblySet(
                source=set_asm,
                components=cmpts,
                version=i,
                current_version=v,
            )
        )

    return specimen_sets
