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


class AssemblySetError(Exception):
    """
    Missing or unexpected data in assemblies and assembly sets.
    """


class AssemblySet:
    __PRINCIPALS = {"hap1", "haploid", "primary"}

    def __init__(
        self,
        *,
        client: TolClient,
        source: DataObject,
        components: list[DataObject],
        version: int,
        current_version: int,
    ):
        self.client = client
        self.source = source
        self.components = components
        self.version = version
        self.current_version = current_version

    def insert_one(self, table: str, obj: DataObject):
        obj_list = self.client.ads.insert(table, [obj])
        if len(obj_list) == 1:
            return obj_list[0]
        msg = (
            "Expected 1 object from ApiDataSource.insert()"
            f" {table} but got {len(obj_list)}"
        )
        raise AssemblySetError(msg)

    def create_or_update(self) -> None:
        set_id = self.stored_assembly_set().id
        if set_id is None:
            msg = f"Failed to store and fetch id for assembly set {self.source.name!r}"
            raise AssemblySetError(msg)
        for cmpt in self.components:
            self.store_assembly_source_assn(cmpt, set_id)
        self.update_component_fields()
        self.client.ads.upsert("assembly", self.components)

    def update_component_fields(self) -> None:
        """
        Updates fields if missing:

        - category (always "release")
        - component_type
        - level?

        Reset:

        - is_principal
        - is_reference
        """

        self.set_release_category_where_missing()
        self.set_component_types()
        self.set_principal_and_reference()

    def set_principal_and_reference(self):
        have_principal = False
        for asm in self.components:
            if asm.component_type.id in self.__PRINCIPALS:  # ty: ignore[unresolved-attribute]
                if have_principal:
                    cmp_types = [x.component_type.id for x in self.components]  # ty: ignore[unresolved-attribute]
                    msg = (
                        "More than one principal component type"
                        f" in assembly set: {cmp_types}"
                    )
                    raise AssemblySetError(msg)
                asm.is_principal = True  # ty: ignore[unresolved-attribute]
                have_principal = True
                asm.is_reference = self.version == self.current_version  # ty: ignore[unresolved-attribute]
            else:
                asm.is_principal = False  # ty: ignore[unresolved-attribute]
                asm.is_reference = False  # ty: ignore[unresolved-attribute]

    def set_release_category_where_missing(self) -> None:
        for asm in self.components:
            if not asm.category:
                asm.category = self.client.build_cdo(  # ty: ignore[unresolved-attribute]
                    "assembly_category", "release", {}
                )

    def set_component_types(self) -> None:
        cmpts = self.components
        cmpt_types = set()
        for asm in cmpts:
            cmpt_types.add(
                typ_obj.id
                if (typ_obj := asm.component_type)
                else self.set_component_type_for_assembly(asm)
            )

        if len(cmpts) == 1 and len(cmpt_types) == 1:
            cmpts[0].component_type = self.client.build_cdo(  # ty: ignore[unresolved-attribute]
                "assembly_component_type", "haploid", {}
            )
        else:
            if "haploid" in cmpt_types:
                for asm in cmpts:
                    if asm.component_type.id == "haploid":  # ty: ignore[unresolved-attribute]
                        self.set_component_type_for_assembly(asm)

    def set_component_type_for_assembly(self, asm: DataObject) -> str:
        desc = asm.description
        if not desc:
            msg = (
                "No description attached to"
                f" Assembly(id = {asm.id!r}, name = {asm.name!r})"
            )
            raise AssemblySetError(msg)

        if m := re.search(r"(?:\.(hap\d+))?\.\d+\s+(\S+)", desc):
            hap, asm_word = m.groups()
        else:
            msg = (
                f"Unexpected wording of assembly.description"
                f" {desc!r} Assembly(id = {asm.id!r}, name = {asm.name!r})"
            )

        if hap:
            typ = hap
        elif asm_word == "assembly":
            typ = "primary"
        elif asm_word == "MT":
            typ = "mitochondrion"
        elif asm_word in {
            "alternate",
            "chloroplast",
            "mitochondrion",
            "maternal",
            "paternal",
        }:
            typ = asm_word
        else:
            msg = f"Cannot determine component_type from description: {desc!r}"
            raise AssemblySetError(msg)
        asm.component_type = self.client.build_cdo("assembly_component_type", typ, {})  # ty: ignore[unresolved-attribute]

        return typ

    def store_assembly_source_assn(self, asm: DataObject, set_id: str) -> None:
        src_assn = None
        if asm.source_assn:
            for assn in asm.source_assn:
                if (src := assn.source) and src.id == set_id:
                    src_assn = assn
                    break
        if not src_assn:
            src_assn = self.insert_one(
                "assembly_source",
                self.client.build_cdo(
                    "assembly_source",
                    None,
                    {
                        "assembly_id": asm.id,
                        "source_assembly_id": set_id,
                    },
                ),
            )
        if asm.source_assn:
            asm.source_assn.append(src_assn)
        else:
            asm.source_assn = [src_assn]  # ty: ignore[unresolved-attribute]

    def stored_assembly_set(self) -> DataObject:
        if not self.source.id:
            self.source = self.insert_one("assembly", self.source)
        return self.source


ENA_PUBLIC = {"status_history.status_type.id": {"eq": {"value": "ENA Public"}}}


def build_assembly_sets(client: TolClient, specimens: list[str] | None = None):
    if not specimens:
        specimens = list_specimens_with_incomplete_assembly_sets(client)

    # ups = TableUpserter(client)

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
            try:
                for asm_set in build_assembly_sets_for_specimen(client, spmn, assemblies):
                        asm_set.create_or_update()
            except AssemblySetError as ase:
                log.warning(f"Error building assembly set for {spmn}:")
                for msg in ase.args:
                    log.warning(f"  {msg}")

                # flat_list = [core_data_object_to_dict(x) for x in asm_set.components]
                # ups.build_table_upserts("assembly", flat_list)

    # ups.apply_upserts()
    # ups.page_results()


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
            raise AssemblySetError(msg)
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
                    ### Error when using empty cdo's:
                    # "specimen": client.build_cdo("specimen", specimen, {}),
                    # "component_type": client.build_cdo(
                    #     "assembly_component_type", "set", {}
                    # ),
                },
            )
        specimen_sets.append(
            AssemblySet(
                client=client,
                source=set_asm,
                components=cmpts,
                version=i,
                current_version=v,
            )
        )

    return specimen_sets
