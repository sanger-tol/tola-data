import sys

import click

from tola import click_options
from tola.ndjson import ndjson_row
from tola.pretty import bold, colour_pager, s
from tola.terminal import pretty_cdo_itr
from tola.tqc.engine import (
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
@click.option(
    "--skip-existing",
    "--skip",
    flag_value=True,
    default=False,
    show_default=True,
    help="""Do not create new objects whose key field matches
      an existing database object.""",
)
@click_options.input_files
def add(ctx, table, key, apply_flag, input_files, skip_existing):
    """Add new rows to a table from ND-JSON input

    INPUT_FILES is a list of files in ND-JSON format.

    A primary key for each row can be provided under the key `<table>.id`.

    Specify a `--key` argument for the name of the field to use to find
    existing rows if not using the default of a primary key field named
    `<table>.id` or if the primary key field is an auto-incrementing
    integer.
    """

    client = ctx.obj
    input_obj = input_objects_or_exit(ctx, input_files)
    return add_rows(client, table, input_obj, key, apply_flag, skip_existing)


def add_rows(client, table, input_obj, key="id", apply_flag=False, skip_existing=False):
    pk = f"{table}.id"
    if key == "id":
        key = pk

    # Check that all the input objects have a value for the key which will be
    # used to fetch the created objects.
    key_id_list = check_key_values_or_exit(input_obj, key, pk)

    if key_id_list and (
        db_obj_before := key_list_search(client, table, key, key_id_list)
    ):
        # Existing objects in database
        if skip_existing:
            input_obj = [x for x in input_obj if not db_obj_before.get(x[key])]
        else:
            plural = s(db_obj_before)
            sys.exit(
                f"Error: {len(db_obj_before)} row{plural} present in database"
                f" with matching '{key}' value{plural}: {sorted(db_obj_before)}"
            )

    if not apply_flag:
        count = len(input_obj)
        n_before = len(db_obj_before)
        existing = (
            (
                f"(Skipped {bold(n_before)} row{s(n_before)}"
                f" with matching '{key}' in database)\n"
            )
            if n_before
            else ""
        )
        click.echo(
            f"Dry run. Use '--apply' flag to store {bold(count)} new row{s(count)}.\n"
            + existing
        )
        return

    # Build CoreDataObjects and add them
    ads = client.ads
    create = dicts_to_core_data_objects(ads, table, input_obj)
    new_obj = ads.insert(table, create)

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


def check_key_values_or_exit(input_obj, key, pk):
    key_values_seen = {}
    null_key_count = 0
    for inp in input_obj:
        v = inp.get(key)
        if v is None:
            null_key_count += 1
        else:
            key_values_seen[v] = 1 + key_values_seen.get(v, 0)
    distinct_keys = len(key_values_seen)
    input_count = len(input_obj)

    if distinct_keys:
        if distinct_keys == input_count:
            # If we have the same number of distinct input keys as
            # objects, return the list of keys
            return list(key_values_seen)
        else:
            multi = "".join(f"  '{k}' = {n}\n" for k, n in key_values_seen if n > 1)
            sys.exit(f"Some '{key}' values occur more than once:\n{multi}")
    elif key == pk:
        if null_key_count == input_count:
            # It's OK if all the objects are missing values for the primary
            # key (assuming that it's auto-incremented).
            return None
        else:
            sys.exit(
                f"Of {input_count} input objects {null_key_count} have {pk} = NULL"
            )
