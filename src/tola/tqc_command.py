import datetime
import io
import json
import logging
import pathlib
import sys

from types import SimpleNamespace

import click

from tol.core import DataSourceFilter

from tola import tolqc_client
from tola.db_connection import ConnectionParamsException
from tola.ndjson import ndjson_row, parse_ndjson_stream

opt = SimpleNamespace(
    table=click.option(
        "--table",
        required=True,
        help="Name of table in ToLQC database",
    ),
    key=click.option(
        "--key",
        default="id",
        show_default=True,
        help=(
            "Column name use to uniquely identify rows."
            # " Defaults to the json:api `id` column"
        ),
    ),
    file=click.option(
        "--file",
        "file_list",
        type=click.Path(
            path_type=pathlib.Path,
            exists=True,
            readable=True,
        ),
        multiple=True,
        help="Input file names.",
    ),
    input_files=click.argument(
        "input_files",
        nargs=-1,
        required=False,
        type=click.Path(
            path_type=pathlib.Path,
            exists=True,
            readable=True,
        ),
    ),
    id_list=click.argument(
        "id_list",
        nargs=-1,
        required=False,
    ),
    apply_flag=click.option(
        "--apply/--dry",
        "apply_flag",
        default=False,
        show_default=True,
        help="Apply changes or perform a dry run and show changes which would be made.",
    ),
)


@click.group()
@tolqc_client.tolqc_alias
@tolqc_client.tolqc_url
@tolqc_client.api_token
@tolqc_client.log_level
@click.pass_context
def cli(ctx, tolqc_alias, tolqc_url, api_token, log_level):
    """Show and update rows and columns in the ToLQC database"""
    logging.basicConfig(level=getattr(logging, log_level))
    try:
        ctx.obj = tolqc_client.TolClient(tolqc_url, api_token, tolqc_alias)
    except ConnectionParamsException as cpe:
        if sys.stdout.isatty():
            # Show help if we're on a TTY
            err = "Error: " + bold("\n".join(cpe.args))
            sys.exit(ctx.get_help() + "\n\n" + err)
        else:
            sys.exit("\n".join(cpe.args))


@cli.command
@click.pass_obj
@opt.table
@opt.key
@opt.id_list
@opt.file
@tolqc_client.file_format
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
@opt.apply_flag
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

    id_list = tuple(id_iterator(key, id_list, file_list, file_format))
    fetched = fetch_list_or_exit(client, table, key, id_list)

    ads = client.ads
    obj_factory = ads.data_object_factory
    if set_value:
        py_value = convert_type(set_value)
        updates = []
        changes = []
        for obj in fetched:
            val = getattr(obj, column_name)
            logging.debug(f"{obj.attributes}")
            oid = getattr(obj, key)

            # Would value be changed?
            if val != py_value:
                updates.append(
                    obj_factory(table, id_=obj.id, attributes={column_name: py_value})
                )
                changes.append({key: oid, column_name: (val, py_value)})

        if updates:
            if apply_flag:
                ads.upsert(table, updates)

            if sys.stdout.isatty():
                click.echo_via_pager(pretty_changes_itr(changes, apply_flag))
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
            click.echo_via_pager(pretty_dict_itr(show_data, key))
        else:
            for row in show_data:
                sys.stdout.write(ndjson_row(row))


@cli.command
@click.pass_context
@opt.table
@opt.key
@opt.apply_flag
@opt.input_files
def edit_rows(ctx, table, key, apply_flag, input_files):
    """Populate or update rows in a table from ND-JSON input

    INPUT_FILES is a list of files in ND-JSON format. Each row is expected to
    contain a value for the key used to identify each record. Any other
    values given will be used to update columns for the record.
    """

    if key == "id":
        key = f"{table}_id"

    input_obj = input_objects_or_exit(ctx, input_files)

    client = ctx.obj
    ads = client.ads
    obj_factory = ads.data_object_factory
    id_list = [x[key] for x in input_obj]
    db_obj = fetch_list_or_exit(client, table, key, id_list)
    flat_obj = [core_data_object_to_dict(x) for x in db_obj]
    updates = []
    changes = []
    for inp, flat, obj in zip(input_obj, flat_obj, db_obj, strict=True):
        attr = {}
        chng = {key: inp[key]}
        for k, inp_v in inp.items():
            if k != key:
                flat_v = flat.get(k)
                if inp_v != flat_v:
                    attr[k] = inp_v
                    chng[k] = flat_v, inp_v
        if attr:
            changes.append(chng)
            updates.append(obj_factory(table, id_=obj.id, attributes=attr))
    if updates:
        if apply_flag:
            for chunk in client.pages(updates):
                ads.upsert(table, chunk)
        if sys.stdout.isatty():
            click.echo_via_pager(pretty_changes_itr(changes, apply_flag))
        else:
            for chng in changes:
                sys.stdout.write(ndjson_row(chng))
            if not apply_flag:
                dry_warning(len(updates))


@cli.command
@click.pass_context
@opt.table
@opt.key
@opt.apply_flag
@opt.input_files
def add(ctx, table, key, apply_flag, input_files):
    """Add new rows to a table from ND-JSON input

    INPUT_FILES is a list of files in ND-JSON format.

    A primary key for each record can be provided under the key `<table>_id`.

    If the database records being created have an auto-incremented integer
    primary key, a `--key` argument giving the key of a parent to-one
    relation is required so that they can be fetched after creation.
    """

    client = ctx.obj
    pk = f"{table}_id"
    if key == "id":
        key = pk

    input_obj = input_objects_or_exit(ctx, input_files)

    # Check that all the input objects have a value for the key which will be
    # used to fetch the created objects.
    check_key_values_or_exit(input_obj, key, pk)

    # List of keys to search on from input objects
    key_id_list = sorted({v for x in input_obj if (v := x.get(key)) is not None})

    # Existing objects in datbase
    db_obj_before = key_list_search(client, table, key_id_list, key)

    # Guard against updating records (via upsert) with same primary key
    if db_obj_before and key == pk:
        plural = s(db_obj_before)
        sys.exit(
            f"Error: {len(db_obj_before)} record{plural} present in"
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
    obj_factory = ads.data_object_factory
    create = []
    for inp in input_obj:
        if oid := inp.get(pk):
            attr = {x: inp[x] for x in inp if x != pk}
            cdo = obj_factory(table, id_=oid, attributes=attr)
        else:
            cdo = obj_factory(table, attributes=inp)
        create.append(cdo)

    ads.upsert(table, create)

    # Fetch objects from database and filter newly created
    db_obj_after = key_list_search(client, table, key_id_list, key)
    new_ids = db_obj_after.keys() - db_obj_before.keys()
    new_obj = [db_obj_after[x] for x in db_obj_after if x in new_ids]

    # Check we created the expected number of new objects
    n_inp = len(input_obj)
    n_new = len(new_obj)
    if n_new != n_inp:
        sys.exit(
            f"Error: Created {n_new} record{s(n_new)}"
            f" from {n_inp} input object{s(n_inp)}.\n"
            "       Existing database records may have been edited."
        )

    if sys.stdout.isatty():
        click.echo_via_pager(pretty_cdo_itr(new_obj, key))
    else:
        for cdo in new_obj:
            sys.stdout.write(ndjson_row(core_data_object_to_dict(cdo)))


@cli.command
@click.pass_obj
@opt.table
@opt.key
@opt.file
@tolqc_client.file_format
@opt.id_list
def show(client, table, key, file_list, file_format, id_list):
    """Show rows from a table in the ToLQC database

    ID_LIST is a list of IDs to operate on, which can additionally be provided
    in --file arugments, or alternatively piped to STDIN.

    Output is in human readable format if STOUT is a terminal, or ND-JSON if
    redirected to a file or UNIX pipe.
    """

    id_list = tuple(id_iterator(key, id_list, file_list, file_format))
    fetched = fetch_all(client, table, key, id_list)
    if sys.stdout.isatty():
        click.echo_via_pager(pretty_cdo_itr(fetched, key))
    else:
        for cdo in fetched:
            sys.stdout.write(ndjson_row(core_data_object_to_dict(cdo)))


def input_objects_or_exit(ctx, input_files):
    if not input_files and sys.stdin.isatty():
        err = "Error: " + bold("Missing INPUT_FILES arguments or STDIN input")
        sys.exit(ctx.get_help() + "\n\n" + err)

    input_obj = []
    if input_files:
        for file in input_files:
            with file.open() as fh:
                input_obj.extend(parse_ndjson_stream(fh))
    else:
        input_obj.extend(parse_ndjson_stream(sys.stdin))

    if not input_obj:
        sys.exit("No input objects")

    return input_obj


def key_list_search(client, table, key_id_list, key):
    db_obj_found = {}
    if key_id_list:
        search_key = "id" if key == f"{table}_id" else key
        for req_list in client.pages(key_id_list):
            filt = DataSourceFilter(in_list={search_key: req_list})
            for cdo in client.ads.get_list(table, object_filters=filt):
                db_obj_found[cdo.id] = cdo

    return db_obj_found


def check_key_values_or_exit(input_obj, key, pk):
    key_found = 0
    for inp in input_obj:
        if inp.get(key):
            key_found += 1
    if key_found != len(input_obj):
        key_type = "Primary" if key == pk else "Parent"
        if key == pk:
            key_type = "Primary"
            poss_err = "\nMissing `--key` argument for parent to-one relation?"
        else:
            key_type = "Parent"
            poss_err = ""
        i_count = len(input_obj)
        sys.exit(
            f"{key_type} key field '{key}' missing in {i_count - key_found}"
            f" out of {i_count} object{s(i_count)} in input.{poss_err}"
        )


def dry_warning(count):
    return (
        f"Dry run. Use '--apply' flag to store {bold(count)} changed row{s(count)}.\n"
    )


def null_if_none(val):
    return "null" if val is None else val


def fetch_list_or_exit(client, table, key, id_list):
    """
    Fetches all the records for `id_list` in the same order, or exits with an
    error.
    """

    key_fetched = key_list_search(client, table, id_list, key)

    # Check if we found a data record for each name_root
    if missed := set(id_list) - key_fetched.keys():
        sys.exit(
            f"Error: Failed to fetch records for {table}.{key} in: {sorted(missed)}"
        )

    # Return objects in the order they were requested
    return [key_fetched[x] for x in id_list]


def fetch_all(client, table, key, id_list):
    if id_list:
        fetched = []
        for req_list in client.pages(id_list):
            filt = DataSourceFilter(in_list={key: req_list})
            fetched.extend(list(client.ads.get_list(table, object_filters=filt)))
        return fetched
    else:
        return list(client.ads.get_list(table))


def cdo_type_id(cdo):
    return f"{cdo.type}_id"


def core_data_object_to_dict(cdo):
    """Flattens a CoreDataObject to a dict"""

    # The object's ID
    flat = {cdo_type_id(cdo): cdo.id}

    # The IDs of the object's to-one related objects
    for rel_name in cdo.to_one_relationships:
        flat[f"{rel_name}_id"] = rltd.id if (rltd := getattr(cdo, rel_name)) else None

    # The object's attributes
    for k, v in cdo.attributes.items():
        if k in ("modified_at", "modified_by"):
            continue
        flat[k] = v

    return flat


def pretty_cdo_itr(cdo_list, key):
    if not cdo_list:
        return []

    cdo_key = cdo_type_id(cdo_list[0])
    flat_list = [core_data_object_to_dict(x) for x in cdo_list]
    return pretty_dict_itr(flat_list, key, cdo_key)


def pretty_dict_itr(row_list, key, alt_key=None):
    if not row_list:
        return []

    first = row_list[0]
    max_hdr = max(len(k) for k in first)

    if key not in first and key == "id":
        if alt_key in first:
            key = alt_key
        else:
            sys.exit(
                f"Possible key values '{key}' or '{alt_key}' not found in first row:\n"
                + json.dumps(first, indent=4)
            )

    yield f"Found {bold(len(row_list))} records:\n"

    for flat in row_list:
        fmt = io.StringIO()
        fmt.write("\n")
        for k, v in flat.items():
            v, style = field_style(v)
            if k == key:
                style = bold_green

            fmt.write(f" {k:>{max_hdr}}  {style(v)}\n")
        yield fmt.getvalue()


def pretty_changes_itr(changes, apply_flag):
    n_changes = len(changes)
    verb = "Made" if apply_flag else "Found"
    yield f"{verb} {bold(n_changes)} change{s(n_changes)}:\n"

    for chng in changes:
        key, *v_keys = chng.keys()
        fmt = io.StringIO()
        fmt.write(f"\n{key}  {bold(chng[key])}\n")

        v_key_max = max(len(x) for x in v_keys)
        old_values = []
        new_values = []
        for k in v_keys:
            old, new = chng[k]
            old_values.append(field_style(old))
            new_values.append(field_style(new))
        old_val_max = max(len(x[0]) for x in old_values)

        for k, (old_val, old_style), (new_val, new_style) in zip(
            v_keys, old_values, new_values, strict=True
        ):
            old_fmt = f"{old_style(old_val):>{old_val_max}}"
            new_fmt = new_style(new_val)
            fmt.write(f"  {k:>{v_key_max}}  {old_fmt} to {new_fmt}\n")
        yield fmt.getvalue()

    if not apply_flag:
        yield "\n" + dry_warning(len(changes))


def field_style(val):
    if val == "":
        return "<empty_string>", bold_red
    if val is None:
        return "null", dim
    if isinstance(val, datetime.date):
        return val.isoformat(), bold
    return repr(val), bold


def id_iterator(key, id_list=None, file_list=None, file_format=None):
    if id_list:
        for oid in id_list:
            yield convert_type(oid)

    if file_list:
        for file in file_list:
            fmt = file_format or guess_file_type(file)
            with file.open() as fh:
                if fmt == "NDJSON":
                    for oid in ids_from_ndjson_stream(key, fh):
                        yield oid
                else:
                    for oid in parse_id_list_stream(fh):
                        yield oid
    elif not (id_list or sys.stdin.isatty()):
        # No IDs or files given on command line, and input is not attached to
        # a terminal, so read from STDIN.
        if file_format == "NDJSON":
            for oid in ids_from_ndjson_stream(key, sys.stdin):
                yield oid
        else:
            for oid in parse_id_list_stream(sys.stdin):
                yield oid


def dim(txt):
    return click.style(txt, dim=True)


def bold_green(txt):
    return click.style(txt, bold=True, fg="green")


def bold_red(txt):
    return click.style(txt, bold=True, fg="red")


def bold(txt):
    return click.style(txt, bold=True)


def guess_file_type(file):
    extn = file.suffix.lower()
    return "NDJSON" if extn == ".ndjson" else "TXT"


def parse_id_list_stream(fh):
    for line in fh:
        yield line.strip()


def ids_from_ndjson_stream(key, fh):
    for row in parse_ndjson_stream(fh):
        oid = row[key]
        if oid is not None:
            yield oid


def convert_type(txt):
    """
    Values given on the command line are always strings.

    'null' is converted to `None`.

    If conversion to `int` or `float` works, return that, or else return the
    original string.
    """
    if txt == "null":
        return None
    else:
        try:
            return int(txt)
        except ValueError:
            try:
                return float(txt)
            except ValueError:
                return txt


def s(x):
    """Formatting plurals. Argument can be an `int` or an iterable"""
    n = x if isinstance(x, int) else len(x)
    return "" if n == 1 else "s"
