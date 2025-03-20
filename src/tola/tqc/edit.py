import sys

import click

from tola import click_options
from tola.ndjson import ndjson_row
from tola.pretty import colour_pager
from tola.tqc.engine import (
    convert_type,
    core_data_object_to_dict,
    dicts_to_core_data_objects,
    dry_warning,
    fetch_list_or_exit,
    id_iterator,
    input_objects_or_exit,
    pretty_changes_itr,
    pretty_dict_itr,
)


@click.command()
@click.pass_obj
@click_options.table
@click_options.key
@click_options.id_list
@click_options.file
@click_options.file_format
@click.option(
    "--column-name",
    "--col",
    help="Name of column to edit",
    required=True,
)
@click.option(
    "--set-value",
    "--set",
    help="Value to set column to",
)
@click_options.apply_flag
def edit_col(
    client,
    table,
    key,
    id_list,
    file_list,
    file_format,
    column_name,
    set_value,
    apply_flag,
):
    """Show or set the value of a column for a list of IDs.

    ID_LIST is a list of IDs to operate on, which can additionally be provided
    in --file arugments, or alternatively piped to STDIN.
    """

    if key == "id":
        key = f"{table}.id"
    id_list = tuple(id_iterator(key, id_list, file_list, file_format))
    fetched = fetch_list_or_exit(client, table, key, id_list)

    ads = client.ads
    if set_value:
        # Leave value as a string if it is a .id field
        py_value = set_value if key.endswith(".id") else convert_type(set_value)
        updates = []
        changes = []
        for obj in fetched:
            flat = core_data_object_to_dict(obj)
            oid = flat[key]
            val = flat.get(column_name)

            # Would value be changed?
            if val != py_value:
                updates.append({key: oid, column_name: py_value})
                changes.append({key: oid, column_name: (val, py_value)})

        if updates:
            if apply_flag:
                ads.upsert(table, dicts_to_core_data_objects(ads, table, updates))

            if sys.stdout.isatty():
                colour_pager(pretty_changes_itr(changes, apply_flag))
            else:
                for row in changes:
                    sys.stdout.write(ndjson_row(row))
                if not apply_flag:
                    click.echo(dry_warning(len(updates)), err=True)

    elif fetched:
        show_data = []
        for obj in fetched:
            val = obj.attributes.get(column_name)
            oid = getattr(obj, key)
            show_data.append({key: oid, column_name: val})
        if sys.stdout.isatty():
            colour_pager(pretty_dict_itr(show_data, key))
        else:
            for row in show_data:
                sys.stdout.write(ndjson_row(row))


@click.command()
@click.pass_context
@click_options.table
@click_options.key
@click_options.apply_flag
@click_options.input_files
def edit_rows_cli(ctx, table, key, apply_flag, input_files):
    """Populate or update rows in a table from ND-JSON input

    INPUT_FILES is a list of files in ND-JSON format. Each line is expected to
    contain a value for the key used to identify a row. Any other values
    given will be used to update columns for the row.
    """

    client = ctx.obj
    input_obj = input_objects_or_exit(ctx, input_files)
    edit_rows(client, table, input_obj, key, apply_flag)


def edit_rows(client, table, input_obj, key="id", apply_flag=False):
    if key == "id":
        key = f"{table}.id"

    # Modification metadata is not editable
    ignore = {"modified_by", "modified_at"}

    id_list = [x[key] for x in input_obj]
    db_obj = fetch_list_or_exit(client, table, key, id_list)
    flat_obj = [core_data_object_to_dict(x) for x in db_obj]
    updates = []
    changes = []
    for inp, flat, obj in zip(input_obj, flat_obj, db_obj, strict=True):
        attr = {}
        chng = {key: inp[key]}
        for k, inp_v in inp.items():
            if k in ignore:
                continue
            if k != key:
                flat_v = flat.get(k)
                if inp_v != flat_v:
                    attr[k] = inp_v
                    chng[k] = flat_v, inp_v
        if attr:
            changes.append(chng)
            updates.append({f"{table}.id": obj.id, **attr})
    if updates:
        if apply_flag:
            ads = client.ads
            for chunk in client.pages(dicts_to_core_data_objects(ads, table, updates)):
                ads.upsert(table, chunk)
        if sys.stdout.isatty():
            colour_pager(pretty_changes_itr(changes, apply_flag))
        else:
            for chng in changes:
                sys.stdout.write(ndjson_row(chng))
            if not apply_flag:
                click.echo(dry_warning(len(updates)), err=True)
