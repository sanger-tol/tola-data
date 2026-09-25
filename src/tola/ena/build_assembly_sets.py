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


def build_assembly_sets(client: TolClient, specimens: list[DataObject] | None = None):
    if not specimens:
        specimens = list_specimens_with_incomplete_assembly_sets(client)
    for chapter in client.pages(specimens):
        specimen_asm: dict[str, DataObject] = {}
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
                "source_assembly_assn.source",
                # "is_principal",
            ],
        ):
            spmn: str = asm.specimen.id  # ty: ignore[unresolved-attribute]
            specimen_asm.setdefault(spmn, []).append(asm)  # ty: ignore[no-matching-overload]
        for spmn, assemblies in specimen_asm.items():
            update_or_create_assembly_set(client, spmn, assemblies)


def list_specimens_with_incomplete_assembly_sets(client: TolClient) -> list[DataObject]:
    specimens = set()
    for asm in client.ads_get_list(
        "assembly",
        filter_spec={
            **ENA_PUBLIC,
            "source_assembly_assn.source.id": {"exists": {"negate": True}},
            "specimen.id": {"exists": {}},
        },
        requested_fields=["id"],
    ):
        specimens.add(asm.specimen.id)  # ty: ignore[unresolved-attribute]

    return sorted(specimens)


def update_or_create_assembly_set(
    client: TolClient, specimen: str, assemblies: list[DataObject]
):
    version_sets: dict[int, list[DataObject]] = {}
    for asm in assemblies:
        m = re.search(r"^\S+\.(\d+)", asm.name)  # ty: ignore[no-matching-overload]
        if not m:
            log.warning(
                f"Failed to parse version from {specimen!r} assembly name {asm.name!r}"
            )
            return
        i = int(m.group(1))
        version_sets.setdefault(i, []).append(asm)

    for asm_set in version_sets.values():
        out = []
        for asm in asm_set:
            info = {
                "id": asm.id,
                "name": asm.name,
                "desc": asm.description,
                "category": cat.id if (cat := asm.category) else None,
                # "is_principal": asm.is_principal,
                "is_reference": asm.is_reference,
                "set": (
                    src.id
                    if (assn := asm.source_assembly_assn) and (src := assn.source)
                    else None
                ),
            }
            out.append(info)
        print(json.dumps(out, indent=2))
