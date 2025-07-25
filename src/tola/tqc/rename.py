import sys

import click
from tol.core import DataSourceFilter

from tola import click_options
from tola.goat_client import GoaTClient
from tola.pretty import bold
from tola.tqc.engine import (
    core_data_object_to_dict,
    fetch_all,
    hash_dir,
    input_objects_or_exit,
)


class ToLQCRenameError(Exception):
    """Error in ToLQC renaming operation"""


@click.command
@click.pass_context
@click.option(
    "--table",
    "table_names",
    help="Name of table to rename",
    type=click.Choice(
        (
            "species",
            "specimen",
        ),
        case_sensitive=False,
    ),
    multiple=True,
)
@click.option(
    "--ignore-species-check",
    flag_value=True,
    help="Ignore errors when checking species names against the GoAT database.",
    default=False,
    show_default=True,
)
@click_options.input_files
def rename(ctx, table_names, input_files, ignore_species_check):
    """
    Rename ToLQC database entries

    This command is risky! It does not, unlike other tqc commands, preview
    changes that would be made, but applies them immediately.

    INTPUT_FILES is a list of ND-JSON input files where each row is an object
    where the values under each key are arrays of [NEW, OLD] name changes.

    (Values are expected to be [NEW, OLD] not [OLD, NEW] to be compatible with
    the output from `diff-mlwh` which outputs diff values as [MLWH, ToLQC] and
    we usually want to rename to the MLWH value.)
    """

    client = ctx.obj
    input_obj = input_objects_or_exit(ctx, input_files)

    for table in table_names:
        try:
            new_count, old_count = rename_records(
                client, table, input_obj, ignore_species_check
            )
        except ToLQCRenameError as tre:
            err = "Error: " + bold("\n".join(tre.args))
            sys.exit(err)
        if sys.stdout.isatty():
            click.echo(
                f"Created {bold(new_count)} new {table} and deleted {bold(old_count)}",
                err=True,
            )


def rename_records(client, table, input_obj, ignore_species_check):
    # Build a dictionary of renaming operations
    spec_dict = build_spec_dict(table, input_obj)

    # Fetch all the old and existing "new" records
    records_by_id = fetch_entries_from_specs(client, table, spec_dict)

    if table == "species":
        to_many_tables = ("specimen",)
        # If there's no taxon_id in the new species, fetch it from the old
        fill_mising_taxon_id_from_old_species(spec_dict, records_by_id)
        fill_mising_field_from_old_records("location.id", spec_dict, records_by_id)

        # Create new species objects from GoaT via taxon_id (and check scientific
        # name matches the expected new species.id).
        new_records = create_new_species_from_goat(
            client, spec_dict, records_by_id, ignore_species_check
        )

        # Avoid collisions in tolid_prefix
        rename_tolid_prefix_in_old_species(client, spec_dict, records_by_id)
    elif table == "specimen":
        to_many_tables = ("sample", "specimen_status")

        ### Will need code here to handle the offspring table once it is populated

        # Create any new specimens required, copying any field values from the
        # old entries
        fill_mising_field_from_old_records("location.id", spec_dict, records_by_id)
        new_records = create_missing_new_objs_from_old(
            client, table, records_by_id, spec_dict
        )

    # Store new records
    client.ads.upsert(table, new_records)

    # Patch locations
    if table == "species":
        client.ads.upsert("location", [x.location for x in new_records])

    # Switch to-many related objects to point to new records
    for to_many in to_many_tables:
        switch_to_many_entries(client, spec_dict, table, to_many)

    # Delete old edit_... table entries
    delete_old_edit_entries(client, table, spec_dict)

    # Delete old records
    client.ads.delete(table, list(spec_dict))

    return len(new_records), len(spec_dict)


def create_missing_new_objs_from_old(client, table, records_by_id, spec_dict):
    cdo = client.build_cdo

    new_obj = []
    pk_name = f"{table}.id"
    for spec in spec_dict.values():
        new, old = spec[pk_name]
        if records_by_id.get(new):
            # Object with this primary key already exists
            continue
        old_dict = records_by_id[old]
        new_attr = {}
        for k, v in old_dict.items():
            if k == pk_name:
                continue
            new_key = k.replace(".id", "_id")
            # There are some exceptions to the naming of foreign keys
            if new_key == "status_id":
                new_key = f"{table}_{new_key}"
            new_attr[new_key] = v
        new_obj.append(cdo(table, new, new_attr))

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
    check_for_missing_old(table, spec_dict, flat_by_id)

    return flat_by_id


def check_for_missing_old(table, spec_dict, id_dict):
    old_missing = []
    for old in spec_dict:
        if not id_dict.get(old):
            old_missing.append(old)

    if old_missing:
        err = f"{len(old_missing)} old {table} not found: {old_missing!r}\n"
        raise ToLQCRenameError(err)


def fill_mising_taxon_id_from_old_species(spec_dict, species_by_id):
    for old_id, spec in spec_dict.items():
        if spec.get("taxon_id"):
            continue
        old_txn = species_by_id[old_id]["taxon_id"]
        spec["taxon_id"] = [old_txn, old_txn]


def fill_mising_field_from_old_records(field, spec_dict, flat_by_id):
    for old_id, spec in spec_dict.items():
        if spec.get(field):
            continue
        if old_loc := flat_by_id[old_id].get(field):
            spec[field] = [old_loc, old_loc]


def delete_old_edit_entries(client, table, spec_dict):
    ads = client.ads
    edit_table = f"edit_{table}"
    old_edits = ads.get_list(
        edit_table,
        object_filters=DataSourceFilter(in_list={f"{table}_id": list(spec_dict)}),
    )
    ads.delete(edit_table, [x.id for x in old_edits])


def rename_tolid_prefix_in_old_species(client, spec_dict, species_by_id):
    cdo = client.build_cdo

    old_species = [species_by_id[x] for x in spec_dict]
    rename_taxon_id = [
        cdo("species", x["species.id"], {"tolid_prefix": x["tolid_prefix"] + "-deltmp"})
        for x in old_species
        if x["tolid_prefix"] is not None
    ]
    for sp in client.ads.upsert("species", rename_taxon_id):
        species_by_id[sp.id] = core_data_object_to_dict(sp)


def switch_to_many_entries(client, spec_dict, table, many_tbl):
    """
    Switch the foreign key in each `to_many` related object to point to its
    new (renamed) parent.
    """
    ads = client.ads
    cdo = client.build_cdo

    fk_name = f"{table}_id"
    pk_name = f"{table}.id"
    many_pk_name = f"{many_tbl}.id"

    many = [
        core_data_object_to_dict(x)
        for x in client.ads.get_list(
            many_tbl,
            object_filters=DataSourceFilter(in_list={fk_name: list(spec_dict)}),
        )
    ]

    switch_obj = []
    for m in many:
        old_fk = m[pk_name]
        new_fk = spec_dict[old_fk][pk_name][0]
        name = m[many_pk_name]
        switch_obj.append(cdo(many_tbl, name, {fk_name: new_fk}))

    ads.upsert(table, switch_obj)


def create_new_species_from_goat(
    client, spec_dict, species_by_id, ignore_species_check
):
    gc = GoaTClient()
    cdo = client.build_cdo

    err = ""
    new_species = []
    for spec in spec_dict.values():
        new_id = spec["species.id"][0]
        if species_by_id.get(new_id):
            # We already have this species in the database
            continue
        taxon_id = spec["taxon_id"][0]
        gr = gc.one_result_or_none(gc.taxon_id_payload(taxon_id))
        if not gr:
            err += f"No species with {taxon_id = } in GoAT for species {new_id!r}\n"
            continue

        info = gr.make_info()
        goat_species = info.pop("species_id")
        if new_id not in gr.synonyms:
            err += (
                f"Species {new_id!r} with {taxon_id = }"
                f" is named {goat_species!r} in GoaT"
                f" and is not in synonyms: {sorted(gr.synonyms)!r}\n"
            )
            if not ignore_species_check:
                continue

        nso = cdo("species", new_id, info)

        # Copy location.id or create a new Location
        loc_info = {"path": hash_dir(taxon_id, new_id)}
        if loc_id := spec.get("location.id"):
            nso.location = cdo("location", loc_id[0], loc_info)
        else:
            nso.location = client.fetch_or_store_one("location", loc_info, key="path")

        new_species.append(nso)

    if err:
        if ignore_species_check:
            sys.stderr.write(f"Ignoring errors:\n{err}")
        else:
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
            "other": [
                (
                    (
                        "accession.id",
                        "biospecimen_accession",
                    ),
                    True,
                ),
            ],
        },
    }

    fld_names = table_defs[table]["key"]
    pk = fld_names[0]
    other_flds = table_defs[table]["other"]

    renames = {}
    for obj in input_obj:
        rename_id = get_rename_field(fld_names, obj, maybe=True)
        if not rename_id:
            continue
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
