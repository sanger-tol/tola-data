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

from typing import Any

from tol.core.data_object import DataObject

from tola.ndjson import pretty_row
from tola.tolqc_client import TolClient


class AssemblySetHandlerError(Exception):
    """
    Error from an AssemblySetHandler
    """


class AssemblySetHandler:
    def __init__(self, client: TolClient):
        self.client = client

    def load_submitted_set(self, asm_set: dict[str, Any]):
        pass

    def fetch_assembly_set(self, asm_set: dict[str, Any]) -> dict[str, Any] | None:
        filt = {
            "component_type.id": {"eq": {"value": "set"}},
        }
        if set_id := asm_set.get("assembly.id"):
            filt["id"] = {"eq": {"value": set_id}}
        elif set_name := asm_set.get("name"):
            filt["name"] = {"eq": {"value": set_name}}
        else:
            msg = "No 'assembly.id' or 'name' field in input object:\n" + pretty_row(
                asm_set
            )
            raise AssemblySetHandlerError(msg)

        asm_list = [
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
        if not asm_list:
            return None
        elif len(asm_list) == 1:
            return asm_list[0]
        else:
            fld, val = ("assembly.id", set_id) if filt.get("id") else ("name", set_name)
            msg = (
                f"{len(asm_list)} assembly sets found for {fld} = {val!r}:\n"
                + ",\n".join(pretty_row(x) for x in asm_list)
            )
            raise AssemblySetHandlerError(msg)

    def get_cdo_attr(self, cdo: DataObject, name, *fields: str) -> str | None:
        if (val := getattr(cdo, name, None)) is not None:
            return self.get_cdo_attr(val, *fields) if fields else val
        return None

    def extract_cdo_fields(
        self, cdo: DataObject, obj: dict[str, Any], fields: tuple[str, ...]
    ):
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
