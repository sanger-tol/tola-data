import click

from tola import click_options
from tola.goat_client import GoaTClient
from tola.ndjson import ndjson_row
from tola.tqc.engine import core_data_object_to_dict, fetch_all, input_objects_or_exit


class ToLQCRenameError(Exception):
    """Error in ToLQC renaming operation"""


@click.command
@click.pass_context
@click.option(
    "--table",
    help="Name of table to rename",
    type=click.Choice(
        ("species",),
        case_sensitive=False,
    ),
)
@click_options.input_files
def rename(ctx, table, input_files):
    """
    Rename ToLQC database entries

    INTPUT_FILES is a list of ND-JSON input files where each row is an object
    where the values under each key are arrays of [NEW, OLD] name changes.

    (Values are expected to be [NEW, OLD] not [OLD, NEW] to be compatible with
    the output from `diff-mlwh` which outputs diff values as [MLWH, ToLQC] and
    we usually want to rename to the MLWH value.)
    """

    client = ctx.obj
    input_obj = input_objects_or_exit(ctx, input_files)

    if table == "species":
        rename_species(client, input_obj)


def rename_species(client, input_obj):
    renames = build_species_rename_spec(input_obj)
    print(ndjson_row(renames))

    # Fetch all of the old and new species
    species_by_id = fetch_entries_from_specs(client, "species", renames)

    # Abort if any of the "old" species do not exist
    check_for_missing_old(renames, species_by_id)

    # If there's no taxon_id in the new species, fetch it from the old
    fill_mising_taxon_id_from_old_species(renames, species_by_id)

    # Create new species objects from GoaT via taxon_id (and check scientific
    # name matches the expected new species.id).
    new_species = create_new_species_from_goat(client, renames, species_by_id)

    # Store a new species entry
    client.ads.upsert("species", new_species)

    # In any edit_species entries change to the new species_id

    # In specimens, change to new species_id

    # Delete old species entries


def fetch_entries_from_specs(client, table, specs):
    id_list = []
    id_field = f"{table}.id"
    for sp in specs.values():
        id_list.extend(sp[id_field])
    flat_list = [
        core_data_object_to_dict(x) for x in fetch_all(client, table, id_field, id_list)
    ]
    return {x[id_field]: x for x in flat_list}


def check_for_missing_old(specs, id_dict):
    old_missing = []
    for old in specs:
        if not id_dict.get(old):
            old_missing.append(old)

    if old_missing:
        err = f"{len(old_missing)} old species not found: {old_missing!r}\n"
        raise ToLQCRenameError(err)


def fill_mising_taxon_id_from_old_species(spec_dict, species_by_id):
    for old_id, spec in spec_dict.items():
        if spec.get("taxon_id"):
            continue
        old_txn = species_by_id[old_id]["taxon_id"]
        spec["taxon_id"] = [old_txn, old_txn]


def create_new_species_from_goat(client, spec_dict, species_by_id):
    obj_factory = client.ads.data_object_factory
    gc = GoaTClient()

    err = ""
    new_species = []
    for spec in spec_dict.values():
        new_id = spec["species.id"][0]
        if species_by_id.get(new_id):
            continue
        taxon_id = spec["taxon_id"][0]
        info = gc.get_species_info(taxon_id)
        goat_species = info.pop("species_id")
        if new_id != goat_species:
            err += (
                f"Species {new_id!r} with {taxon_id = }"
                f" is named {goat_species!r} in GoaT"
            )
            continue
        new_species.append(obj_factory("species", id_=new_id, attributes=info))
    if err:
        raise ToLQCRenameError(err)

    return new_species


def build_species_rename_spec(input_obj):
    renames = {}
    for obj in input_obj:
        rename_id = get_rename_field(
            (
                "species.id",
                "species_id",
                "scientific_name",
            ),
            obj,
        )
        taxon_change = get_rename_field(("taxon_id",), obj, maybe=True)
        spec = {
            "species.id": rename_id,
            "taxon_id": taxon_change,
        }
        old = rename_id[1]
        if (prev_spec := renames.get(old)) and spec != prev_spec:
            msg = f"Found {prev_spec!r} for {old!r} when storing {spec!r}"
            raise ToLQCRenameError(msg)
        else:
            renames[old] = spec

    return renames


def get_rename_field(field_names, obj: dict, maybe=False):
    """
    Looks for rename fields under a list of field names in a dict and checks
    that they are a list of two different values.

    If the `maybe` parameter is set to `True`, just return `None` if the field
    is missing or empty.
    """

    found = None
    found_fn = None
    for fn in field_names:
        if found := obj.get(fn):
            found_fn = fn  # Save for error messages
            break

    if maybe and found is None:
        return None

    err = None
    if not found_fn:
        err = f"Failed to find any of {field_names!r}"
    elif not isinstance(found, list):
        err = f"Value under {found_fn!r} key is not a list"
    elif len(found) != 2:
        err = f"Expecting 2 values under {found_fn!r} but got {found!r}"
    elif found[0] == found[1]:
        err = f"Expecting a rename but the 2 values {found!r} under {found_fn!r} match"

    if err:
        err += f" in JSON object:\n{obj}"
        raise ToLQCRenameError(err)

    return found
