import sys

import click
from tol.core import DataSourceFilter

from tola import click_options
from tola.goat_client import GoaTClient
from tola.pretty import bold
from tola.tqc.engine import core_data_object_to_dict, fetch_all, input_objects_or_exit


class ToLQCRenameError(Exception):
    """Error in ToLQC renaming operation"""


@click.command
@click.pass_context
@click.option(
    "--table",
    help="Name of table to rename",
    type=click.Choice(
        (
            "species",
            "specimen",
        ),
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
    spec_dict = build_spec_dict(table, input_obj)

    try:
        if table == "species":
            rename_species(client, table, spec_dict)
        elif table == "specimen":
            rename_specimens(client, table, spec_dict)
    except ToLQCRenameError as tre:
        err = "Error: " + bold("\n".join(tre.args))
        sys.exit(err)


def rename_species(client, table, spec_dict):
    # Fetch all of the old and existing "new" species
    species_by_id = fetch_entries_from_specs(client, table, spec_dict)

    # If there's no taxon_id in the new species, fetch it from the old
    fill_mising_taxon_id_from_old_species(spec_dict, species_by_id)

    # Create new species objects from GoaT via taxon_id (and check scientific
    # name matches the expected new species.id).
    new_species = create_new_species_from_goat(client, spec_dict, species_by_id)

    # Avoid collisions in tolid_prefix
    rename_tolid_prefix_in_old_species(client, spec_dict, species_by_id)

    # Store new species entries
    client.ads.upsert(table, new_species)

    # In specimens, change to new species_id
    switch_to_many_entries(client, spec_dict, table, "specimen")

    # Delete any edit_species entries linked to old species
    delete_old_edit_species(client, spec_dict)

    # Delete old species entries
    client.ads.delete(table, list(spec_dict))

    if sys.stdout.isatty():
        new = bold(len(new_species))
        old = bold(len(spec_dict))
        click.echo(f"Created {new} new species and deleted {old}", err=True)


def rename_specimens(client, table, spec_dict):
    # Fetch old and any existing "new" specimens
    spcmns_by_id = fetch_entries_from_specs(client, table, spec_dict)

    # Create any new specimens required, copying all the fields from the old
    # entries
    new_spcmns = create_missing_new_objs_from_old(
        client, table, spcmns_by_id, spec_dict
    )

    # Store the new specimens
    client.ads.upsert(table, new_spcmns)

    # Switch samples to new specimens
    switch_to_many_entries(client, spec_dict, table, "sample")

    ### Will need code here to handle the offspring table once it is populated


def create_missing_new_objs_from_old(client, table, spcmns_by_id, spec_dict):
    ads = client.ads
    obj_bldr = ads.data_object_factory
    new_obj = []
    pk_name = f"{table}.id"
    for spec in spec_dict.values():
        new, old = spec[pk_name]
        if spcmns_by_id.get(new):
            # Object with this primary key already exists
            continue
        old_dict = spcmns_by_id[old]
        new_attr = {}
        for k, v in old_dict.items():
            if k == pk_name:
                continue
            # This will not work for all tables since not
            new_attr[k.replace(".id", "_id")] = v
        new_obj.append(obj_bldr(table, id_=new, attributes=new_attr))

    return new_obj


def fetch_entries_from_specs(client, table, spec_dict):
    id_list = []
    id_field = f"{table}.id"
    for sp in spec_dict.values():
        id_list.extend(sp[id_field])
    flat_list = [
        core_data_object_to_dict(x) for x in fetch_all(client, table, id_field, id_list)
    ]

    flat_by_id = {x[id_field]: x for x in flat_list}

    # Abort if any of the "old" species do not exist
    check_for_missing_old(spec_dict, flat_by_id)

    return flat_by_id


def check_for_missing_old(spec_dict, id_dict):
    old_missing = []
    for old in spec_dict:
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


def delete_old_edit_species(client, spec_dict):
    ads = client.ads
    old_edits = ads.get_list(
        "edit_species",
        object_filters=DataSourceFilter(in_list={"species_id": list(spec_dict)}),
    )
    ads.delete("edit_species", [x.id for x in old_edits])


def rename_tolid_prefix_in_old_species(client, spec_dict, species_by_id):
    ads = client.ads
    obj_bldr = ads.data_object_factory
    old_species = [species_by_id[x] for x in spec_dict]
    rename_taxon_id = [
        obj_bldr(
            "species",
            id_=x["species.id"],
            attributes={"tolid_prefix": x["tolid_prefix"] + "-deltmp"},
        )
        for x in old_species
    ]
    for sp in ads.upsert("species", rename_taxon_id):
        species_by_id[sp.id] = core_data_object_to_dict(sp)


def switch_to_many_entries(client, spec_dict, table, many_tbl):
    ads = client.ads
    obj_bldr = ads.data_object_factory

    pk_name = f"{table}.id"
    fk_name = f"{table}_id"
    many_pk_name = f"{many_tbl}.id"

    many = [
        core_data_object_to_dict(x)
        for x in client.ads.get_list(
            many_tbl,
            object_filters=DataSourceFilter(in_list={fk_name: list(spec_dict)}),
        )
    ]
    old_new = {x: y[pk_name][0] for x, y in spec_dict.items()}
    switch_obj = [
        obj_bldr(many_tbl, id_=x[many_pk_name], attributes={fk_name: old_new[x[pk_name]]})
        for x in many
    ]
    ads.upsert(table, switch_obj)


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


def build_spec_dict(table, input_obj):
    table_defs = {
        "species": {
            "key": (
                "species.id",
                "species_id",
                "scientific_name",
            ),
            "other": [
                ("taxon_id", True),
            ],
        },
        "specimen": {
            "key": (
                "specimen.id",
                "specimen_id",
                "tol_specimen_id",
            ),
            "other": [],
        },
    }

    fld_names = table_defs[table]["key"]
    pk = fld_names[0]
    other_flds = table_defs[table]["other"]

    renames = {}
    for obj in input_obj:
        rename_id = get_rename_field(fld_names, obj)
        spec = {pk: rename_id}
        for fld, maybe in other_flds:
            if isinstance(fld, tuple):
                fld_tuple = fld
                fld = fld_tuple[0]
            else:
                fld_tuple = (fld,)
            spec[fld] = get_rename_field(fld_tuple, obj, maybe=maybe)
        old = rename_id[1]
        if (prev_spec := renames.get(old)) and spec != prev_spec:
            msg = f"Found {prev_spec!r} for {old!r} when building {spec!r}"
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
