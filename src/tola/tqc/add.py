import sys

import click

from tola import click_options
from tola.ndjson import ndjson_row
from tola.pretty import bold, colour_pager, s
from tola.terminal import pretty_cdo_itr
from tola.tqc.engine import (
    check_key_values_or_exit,
    core_data_object_to_dict,
    dicts_to_core_data_objects,
    input_objects_or_exit,
    key_list_search,
)


@click.command()
@click.pass_context
@click_options.table
@click_options.key
@click_options.apply_flag
@click_options.input_files
def add(ctx, table, key, apply_flag, input_files):
    """Add new rows to a table from ND-JSON input

    INPUT_FILES is a list of files in ND-JSON format.

    A primary key for each row can be provided under the key `<table>.id`.

    If the database rows being created have an auto-incremented integer
    primary key, a `--key` argument giving the key of a parent to-one
    relation is required so that they can be fetched after creation.
    """

    client = ctx.obj
    input_obj = input_objects_or_exit(ctx, input_files)
    return add_rows(client, table, input_obj, key, apply_flag)


def add_rows(client, table, input_obj, key="id", apply_flag=False):
    pk = f"{table}.id"
    if key == "id":
        key = pk

    # Check that all the input objects have a value for the key which will be
    # used to fetch the created objects.
    check_key_values_or_exit(input_obj, key, pk)

    # List of keys to search on from input objects
    key_id_list = sorted({v for x in input_obj if (v := x.get(key)) is not None})

    # Existing objects in datbase
    db_obj_before = key_list_search(client, table, key, key_id_list)

    # Guard against updating rows (via upsert) with same primary key
    if db_obj_before and key == pk:
        plural = s(db_obj_before)
        sys.exit(
            f"Error: {len(db_obj_before)} row{plural} present in"
            f" database with matching '{pk}' value{plural}: {sorted(db_obj_before)}"
        )

    if not apply_flag:
        count = len(input_obj)
        click.echo(
            f"Dry run. Use '--apply' flag to store {bold(count)} new row{s(count)}.\n"
        )
        return

    # Build CoreDataObjects and upsert
    ads = client.ads
    create = dicts_to_core_data_objects(ads, table, input_obj)
    ads.upsert(table, create)

    # Fetch objects from database and filter newly created
    db_obj_after = key_list_search(client, table, key, key_id_list)
    new_ids = db_obj_after.keys() - db_obj_before.keys()
    new_obj = [db_obj_after[x] for x in db_obj_after if x in new_ids]

    # Check we created the expected number of new objects
    n_inp = len(input_obj)
    n_new = len(new_obj)
    if n_new != n_inp:
        sys.exit(
            f"Error: Created {n_new} row{s(n_new)}"
            f" from {n_inp} input object{s(n_inp)}.\n"
            "       Existing database rows may have been edited."
        )

    if sys.stdout.isatty():
        colour_pager(pretty_cdo_itr(new_obj, key, head="Created {} new row{}:\n"))
    else:
        for cdo in new_obj:
            sys.stdout.write(ndjson_row(core_data_object_to_dict(cdo)))
