"""
Some different ways to fetch a species from the ToLQC database via the ToL
API, given either a ToLID prefix or a ToL specimen ID.
"""

from tol.core import DataSourceFilter
from tol.sources.tolqc import tolqc

ads = tolqc()

# Via an `eq` on the `tolid_prefix` field of the species:
for species in ads.get_list(
    "species",
    object_filters=DataSourceFilter(
        and_={
            "tolid_prefix": {
                "eq": {"value": "mVulVul"},
            }
        }
    ),
):
    print(f"species = {species.id!r}  taxon_id = {species.taxon_id!r}")

# Querying with a specimen ToLID via the `speciemens` to-many relationship on
# species:
for species in ads.get_list(
    "species",
    object_filters=DataSourceFilter(
        and_={
            "specimens.id": {
                "eq": {"value": "mVulVul1"},
            }
        }
    ),
):
    print(f"species = {species.id!r}  taxon_id = {species.taxon_id!r}")

# Feching a specimen by its ToLID, and fetching its to-one species
# relationship via `requested_fields` to avoid a second round trip to the
# server when calling `specimen.species`:
for specimen in ads.get_list(
    "specimen",
    object_filters=DataSourceFilter(
        and_={
            "id": {
                "in_list": {"value": ["mVulVul1"]},
            }
        },
    ),
    requested_fields=["species"],
):
    print(f"species = {specimen.species.id!r}")

# Using the `get_by_ids` API method, again using `requested_fields` to fetch
# the attached species object in a single server request:
for specimen in ads.get_by_ids(
    "specimen",
    ["mVulVul1"],
    requested_fields=["species"],
):
    print(f"species = {specimen.species.id!r}")
