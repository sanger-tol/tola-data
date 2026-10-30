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

Component specific fields:

- `centre.name` required, and must match an entry in the `centre` table.
- `category.id` should be "release" for "Submitted" assemblies.
- `bioproject_accession.id` required.  The assembly's BioProject accession.
- `genome_accession.id` optional.  Can be set when updating.
- `status.status_type.id` required.  Set to "Submitted" when recording new
  submissions.
- `status.status_time` optional.  Will be set to the current time if omitted.
  The default time zone of "Europe/London" will be applied if not specified.
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

from tola.ndjson import ndjson_row, pretty_row, set_timezone_if_naive
from tola.pretty import s
from tola.tolqc_client import TolClient
from tola.tqc.engine import dicts_to_core_data_objects


class AssemblySetHandlerError(Exception):
    """
    Error from an AssemblySetHandler
    """


type AsmSet = dict[str, Any]
type AsmDict = dict[str, Any]


class AssemblySetHandler:
    def __init__(self, client: TolClient):
        self.client = client

    def load_submitted_set(self, asm_set: AsmSet) -> AsmSet:
        if asm_id := asm_set.get("assembly.id"):
            # Update assembly set
            old_set = self.fetch_one_assembly_set("id", asm_id)
            return self.update_assembly_set(old_set, asm_set)
        else:
            return self.store_new_assembly_set(asm_set)

    def store_new_assembly_set(self, asm_set: AsmSet) -> AsmSet:
        self.validate_required_fields(asm_set)
        return self.store_assembly_set(asm_set)

    def update_assembly_set(self, old_set: AsmSet, new_set: AsmSet) -> AsmSet:
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
                    ),
                )
                components.append(cmpt_obj)
        components.sort(key=lambda x: str(x.get("name")))

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
                    "status.status_type.id",
                    "level",
                    "is_principal",
                ),
                cmpt,
            ):
                return err

        # Turn status time fields into Datetime objects
        for cmpt in components:
            if status_time := cmpt.get("status.status_time"):
                cmpt["status.status_time"] = set_timezone_if_naive(
                    dt.datetime.fromisoformat(status_time)
                )

        # All OK!
        return None

    def __validate_required_dict_fields(
        self, required: tuple[str, ...], asm: AsmDict
    ) -> str | None:
        for field in required:
            if asm.get(field) in (None, ""):
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
                f" already in database: {same_names}"
            )

    def set_specimen_sets_to_non_reference(self, specimen_id: str) -> None:
        update_asm_ids = []
        for asm_set in self.fetch_assembly_sets("specimen.id", specimen_id):
            if asm_set.get("is_reference") is True:
                update_asm_ids.append(asm_set.get("assembly.id"))
        if not update_asm_ids:
            return

        ads = self.client.ads
        cdo_updates = dicts_to_core_data_objects(
            ads,
            "assembly",
            [
                {
                    "assembly.id": asm_id,
                    "is_reference": False,
                }
                for asm_id in update_asm_ids
            ],
        )
        ads.upsert("assembly", cdo_updates)

    def store_assembly_set(self, asm_set: AsmSet) -> AsmSet:
        ads = self.client.ads

        # Prepare assembly set specs
        set_spec = {key: val for key, val in asm_set.items() if key != "components"}

        # Prepare assembly component specs
        cmpt_specs = []
        centre_dict = self.client.centre_name_to_id_dict
        for cmpt in asm_set["components"]:
            spec = {}
            for key, val in cmpt.items():
                if key.startswith("status."):
                    continue
                if key == "centre.name":
                    if centre_id := centre_dict.get(val):
                        spec["centre.id"] = centre_id
                    else:
                        msg = f"No such centre {val!r} in database"
                        raise AssemblySetHandlerError(msg)
                else:
                    spec[key] = val
            cmpt_specs.append(spec)

        # Store set
        (set_obj,) = ads.insert(
            "assembly",
            dicts_to_core_data_objects(ads, "assembly", [set_spec]),
        )

        # Store components.  Use `upsert` since some components may already be
        # stored and have "assembly.id" set.
        cmpt_objs = [
            x
            for x in ads.upsert(
                "assembly",
                dicts_to_core_data_objects(ads, "assembly", cmpt_specs),
            )
            if isinstance(x, DataObject)
        ]

        # Set component statuses
        cmpt_name_ids = {cmpt.name: cmpt.id for cmpt in cmpt_objs}
        status_specs = []
        for cmpt_spec in asm_set["components"]:
            cmpt_asm_id = cmpt_name_ids[cmpt_spec["name"]]
            status_specs.append(
                {
                    "assembly.id": cmpt_asm_id,
                    "status_type.id": cmpt_spec["status.status_type.id"],
                    "status_time": cmpt_spec.get("status.status_time"),
                }
            )
        self.client.ndjson_post(
            "loader/status/assembly", (ndjson_row(x) for x in status_specs)
        )

        # Link components to sets by writing new entries into assembly_components
        link_specs = [
            {
                "source.id": set_obj.id,
                "component.id": cmpt.id,
            }
            for cmpt in cmpt_objs
        ]
        ads.insert(
            "assembly_source",
            dicts_to_core_data_objects(ads, "assembly_source", link_specs),
        )

        # Fetch the new assembly set from the database
        return self.fetch_one_assembly_set("assembly.id", set_obj.id)
