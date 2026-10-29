"""
Example JSON (submitted on single line in the ND-JSON input) for creating and
updating assembly table entries.

```json
{
  "assembly.id": null,
  "name": "mVulVul1.2",
  "description": "Assembly set mVulVul1.2",
  "specimen.id": "mVulVul1",
  "component_type.id": "set",
  "is_reference": true,
  "components": [
    {
      "assembly.id": null,
      "name": "mVulVul1.hap1.2",
      "description": "mVulVul1.hap1.2 assembly for Vulpes vulpes",
      "specimen.id": "mVulVul1",
      "centre.name": "Wellcome Sanger Institute",
      "category.id": "release",
      "component_type.id": "hap1",
      "bioproject_accession.id": "PRJEB75990",
      "genome_accession.id": null,
      "status.status_type.id": "Submitted",
      "status.status_time": "2024-07-02T01:44:20+01:00",
      "level": "chromosome",
      "is_principal": true,
      "is_reference": true
    },
    {
      "assembly.id": null,
      "name": "mVulVul1.hap2.2",
      "description": "mVulVul1.hap2.2 assembly for Vulpes vulpes",
      "specimen.id": "mVulVul1",
      "centre.name": "Wellcome Sanger Institute",
      "category.id": "release",
      "component_type.id": "hap2",
      "bioproject_accession.id": "PRJEB75989",
      "genome_accession.id": null,
      "status.status_type.id": "Submitted",
      "status.status_time": "2024-07-02T01:43:59+01:00",
      "level": "chromosome",
      "is_principal": false,
      "is_reference": false
    }
  ]
}
```

Fields:

- `assembly.id` optional, but required if updating an assembly.  Its presence
  flags that an update is being performed.
- `name` required, and must not exist when creating new entries.
- `description` optional.
- `specimen.id` required.
- `component_type.id` required.  Should be "set" on the top level and set to
  the an ID from the `assembly_component_type` table.  Must be unique within
  a set.
- `is_reference` usually set to true when submitting new assemblies on both
  the set and the principal component of the set.

Component specific fields:

- `centre.name` required, and must match an entry in the `centre` table.
- `category.id` should be "release" for "Submitted" assemblies.
- `bioproject_accession.id` required.  The assembly's BioProject accession.
- `genome_accession.id` optional.  Can be set when updating.
- `status_type.id` required.  Set to "Submitted" when recording new
  submissions.
- `status_time` optional.  Will be set to the current time if omitted.  Must
  be in ISO timestamp with time zone format if specified.
- `level` required.
- `is_principal` required.  Set to `true` for the main assembly and `false`
  for all the others.

Any optional fields with value `null` can be omitted from the input JSON.

If a new component assembly is being added to a set, the existing components
can be listed by providing their `assembly.id` integer values.

"""

import datetime as dt
from typing import Any

from tol.core.data_object import DataObject

from tola.ndjson import pretty_row
from tola.pretty import s
from tola.tolqc_client import TolClient


class AssemblySetHandlerError(Exception):
    """
    Error from an AssemblySetHandler
    """


type AsmSet = dict[str, Any]
type AsmDict = dict[str, Any]


class AssemblySetHandler:
    def __init__(self, client: TolClient):
        self.client = client

    def load_submitted_set(self, asm_set: AsmSet):
        if asm_id := asm_set.get("assembly.id"):
            # Update assembly set
            old_set = self.fetch_one_assembly_set("id", asm_id)
            self.update_assembly_set(old_set, asm_set)
        else:
            self.store_new_assembly_set(asm_set)

    def store_new_assembly_set(self, asm_set: AsmSet) -> None:
        self.validate_required_fields(asm_set)
        # Fetch existing sets by specimen ID if set.is_reference = True

        # Store set

        # Store components

        # Link components to sets by writing new entries into assembly_components

        # Set old specimen ID sets to is_reference = False

    def update_assembly_set(self, old_set: AsmSet, new_set: AsmSet) -> None:
        self.validate_required_fields(new_set)
        # Update any changed fields in set

        # For each component, either store it if it is new (and link it to the
        # set in the assembly_components table) or update its fields.


    def fetch_one_assembly_set(self, field: str, value: str) -> AsmSet:
        asm_list = self.fetch_assembly_sets(field, value)

        if len(asm_list) == 1:
            return asm_list[0]
        else:
            msg = (
                f"{len(asm_list)} assembly sets found for {field} = {value!r}:\n"
                + ",\n".join(pretty_row(x) for x in asm_list)
            )
            raise AssemblySetHandlerError(msg)

    def fetch_assembly_sets_by_spec(self, spec: dict[str, Any]):
        for field in ("assembly.id", "name"):
            if (val := spec.get(field)) is not None:
                return self.fetch_assembly_sets(field, val)

    def fetch_assembly_sets(self, field: str, value: str) -> list[AsmSet]:
        query_field = "id" if field == "assembly.id" else field
        filt = {
            "component_type.id": {"eq": {"value": "set"}},
            query_field: {"eq": {"value": value}},
        }

        return [
            self.obj_to_assembly_set(x)
            for x in self.client.ads_get_list(
                "assembly",
                filter_spec=filt,
                requested_fields=[
                    "name",
                    "description",
                    "is_reference",
                    "component_assn.component.status",
                    "dataset_assn.dataset.data_assn.data",
                ],
            )
        ]

    def get_cdo_attr(self, cdo: DataObject, name, *fields: str) -> str | None:
        if (val := getattr(cdo, name, None)) is not None:
            return self.get_cdo_attr(val, *fields) if fields else val
        return None

    def extract_cdo_fields(self, cdo: DataObject, obj: AsmSet, fields: tuple[str, ...]):
        for fld in fields:
            path = fld.split(".")
            obj[fld] = self.get_cdo_attr(cdo, *path)

    def obj_to_assembly_set(self, cdo: DataObject):
        asm_set = {"assembly.id": cdo.id}
        self.extract_cdo_fields(
            cdo,
            asm_set,
            (
                "name",
                "description",
                "specimen.id",
                "component_type.id",
                "is_reference",
            ),
        )

        components = []
        cmpt_list = getattr(cdo, "component_assn", None)
        if cmpt_list is not None:
            for cmpt_rel in cmpt_list:
                cmpt = cmpt_rel.component
                cmpt_obj = {"assembly.id": cmpt.id}
                self.extract_cdo_fields(
                    cmpt,
                    cmpt_obj,
                    (
                        "name",
                        "description",
                        "specimen.id",
                        "centre.name",
                        "category.id",
                        "component_type.id",
                        "bioproject_accession.id",
                        "genome_accession.id",
                        "status.status_type.id",
                        "status.status_time",
                        "level",
                        "is_principal",
                        "is_reference",
                    ),
                )
                components.append(cmpt_obj)

        asm_set["components"] = components
        return asm_set

    def validate_required_fields(self, asm_set: AsmSet):
        if error := self.__run_validation(asm_set):
            msg = f"Assembly set validattion failed: {error}\n" + pretty_row(asm_set)
            raise AssemblySetHandlerError(msg)

    def __run_validation(self, asm_set: AsmSet) -> str | None:
        components = asm_set.get("components")
        if not components:
            return "Set must contain one or more components"

        # Test naming of set and components
        names: set[str | None] = {asm_set.get("name")}
        for cmpt in components:
            names.add(cmpt.get("name"))

        if None in names:
            return "The set and each component must each have a name"
        if "" in names:
            return "Empty string is not a valid name"
        if len(names) != len(components) + 1:
            return "The set and each component must have unique names"

        if err := self.__check_database_for_new_names(asm_set):
            return err

        # Component types
        if asm_set.get("component_type.id") != "set":
            return "Assembly set component_type.id must be 'set'"
        cmpt_types = {cmpt.get("component_type.id") for cmpt in components}
        if len(cmpt_types) != len(components):
            return "The component_type.id of each component must be different"
        if "set" in cmpt_types:
            return "Component assembly cannot have component_type 'set'"

        # Requried fields
        if err := self.__validate_required_dict_fields(("specimen.id",), asm_set):
            return err
        for cmpt in components:
            if err := self.__validate_required_dict_fields(
                (
                    "specimen.id",
                    "centre.name",
                    "bioproject_accession.id",
                    "status_type.id",
                    "level",
                    "is_principal",
                ),
                cmpt,
            ):
                return err

        # All OK!
        return None

    def __validate_required_dict_fields(
        self, required: tuple[str, ...], asm: AsmDict
    ) -> str | None:
        for field in required:
            if asm.get(field) not in (None, ""):
                return f"Missing value for {field}"
        return None

    def __check_database_for_new_names(self, asm_set: AsmSet) -> str | None:
        """
        Check the database for any pre-existing new names.  Performed after
        checking that each name in the set is unique and is not null or empty
        string.
        """

        # Make list of names where assembly.id is null
        names = [
            x.get("name")
            for x in [asm_set, *asm_set["components"]]
            if x.get("assembly.id") is None
        ]
        same_names = [
            {"id": asm.id, "name": asm.name}
            for asm in self.client.ads_get_list(
                "assembly",
                filter_spec={
                    "name": {
                        "in_list": {"value": list(names)},
                    }
                },
            )
        ]
        if same_names:
            y = "y" if len(same_names) == 1 else "ies"
            return (
                "Names of the set and each of its components must be new."
                f" Assembl{y} with matching name{s(same_names)}"
                f" already in database: {len(same_names)}"
            )
