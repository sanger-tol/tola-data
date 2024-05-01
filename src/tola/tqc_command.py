import io
import logging
import pathlib
import sys

import click

from tol.core import DataSourceFilter

from tola import tolqc_client
from tola.db_connection import ConnectionParamsException
from tola.ndjson import ndjson_row, parse_ndjson_stream

table = click.option(
    "--table",
    required=True,
    help="Name of table in ToLQC database",
)

key = click.option(
    "--key",
    default="id",
    show_default=True,
    help=(
        "Column name use to uniquely identify rows."
        # " Defaults to the json:api `id` column"
    ),
)

file = click.option(
    "--file",
    "file_list",
    type=click.Path(
        path_type=pathlib.Path,
        exists=True,
        readable=True,
    ),
    multiple=True,
    help="Input file names.",
)

id_list = click.argument(
    "id_list",
    nargs=-1,
    required=False,
)

apply_flag = click.option(
    "--apply/--dry",
    "apply_flag",
    default=False,
    show_default=True,
    help="Apply changes or perform a dry run and show changes which would be made.",
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
            err = "Error: " + click.style("\n".join(cpe.args), bold=True)
            sys.exit(ctx.get_help() + "\n\n" + err)
        else:
            raise cpe


@cli.command
@click.pass_obj
@table
@key
@id_list
@file
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
@apply_flag
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
    fetched = fetch_all_unique(client, table, key, id_list)

    ads = client.ads
    ObjFactory = ads.data_object_factory
    if set_value:
        py_value = convert_type(set_value)
        updates = []
        msg = io.StringIO()
        for obj in fetched:
            val = getattr(obj, column_name)
            logging.debug(f"{obj.attributes}")
            oid = getattr(obj, key)

            # Would value be changed?
            if val != py_value:
                updates.append(
                    ObjFactory(table, id_=obj.id, attributes={column_name: py_value})
                )
                msg.write(f"{null_if_none(val)}\t{set_value}\t{oid}\n")

        if updates:
            if apply_flag:
                ads.upsert(table, updates)
            click.echo(f"previous_value\t{table}.{column_name}\t{key}")
            click.echo(msg.getvalue(), nl=False)
            if not apply_flag:
                click.echo("Dry run. Use '--apply' flag to store changes.", err=True)

    elif fetched:
        click.echo(f"{table}.{column_name}\t{key}")
        for obj in fetched:
            val = obj.attributes.get(column_name)
            oid = getattr(obj, key)
            click.echo(f"{null_if_none(val)}\t{oid}")


@cli.command
@click.pass_context
@table
@key
@apply_flag
@click.argument(
    "input_files",
    nargs=-1,
    required=False,
    type=click.Path(
        path_type=pathlib.Path,
        exists=True,
        readable=True,
    ),
)
def edit_rows(ctx, table, key, apply_flag, input_files):
    """Populate or update rows in a table from ND-JSON input

    INPUT_FILES is a list of files in ND-JSON format. Each row is expected to
    contain a value for the key used to identify each record. Any other
    values given will be used to update columns for the record.
    """

    if not input_files and sys.stdin.isatty():
        err = "Error: " + click.style(
            "Missing INPUT_FILES arguments or STDIN input", bold=True
        )
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

    client = ctx.obj
    ads = client.ads
    ObjFactory = ads.data_object_factory
    id_list = list(x[key] for x in input_obj)
    db_obj = fetch_all_unique(client, table, key, id_list)
    id_key = cdo_type_id(db_obj[0])
    flat_obj = list(core_data_object_to_dict(x) for x in db_obj)
    updates = []
    changes = []
    for inp, flat, obj in zip(input_obj, flat_obj, db_obj, strict=True):
        attr = {}
        chng = {}
        for k, inp_v in inp.items():
            if k == id_key:
                chng[id_key] = inp_v
            else:
                flat_v = flat.get(k)
                if inp_v != flat_v:
                    attr[k] = inp_v
                    chng[k] = flat_v, inp_v
        if attr:
            changes.append(chng)
            updates.append(ObjFactory(table, id_=obj.id, attributes=attr))
    if updates:
        if apply_flag:
            ads.upsert(table, updates)
        for chng in changes:
            print(ndjson_row(chng))


@cli.command
@click.pass_obj
@table
@key
@file
@tolqc_client.file_format
@id_list
def show(client, table, key, file_list, file_format, id_list):
    """Show rows from a table in the ToLQC database

    ID_LIST is a list of IDs to operate on, which can additionally be provided
    in --file arugments, or alternatively piped to STDIN.
    """

    id_list = tuple(id_iterator(key, id_list, file_list, file_format))
    fetched = fetch_all(client, table, key, id_list)
    if sys.stdout.isatty():
        click.echo(f"Printing {len(fetched)} records", err=True)
        click.echo_via_pager(pretty_itr(fetched, key))
    else:
        for cdo in fetched:
            sys.stdout.write(ndjson_row(core_data_object_to_dict(cdo)))


def null_if_none(val):
    return "null" if val is None else val


def fetch_all_unique(client, table, key, id_list):
    filt = DataSourceFilter(in_list={key: id_list})
    fetched = list(client.ads.get_list(table, object_filters=filt))
    key_fetched = {}
    if fetched:
        idx_key = "id" if key == cdo_type_id(fetched[0]) else key
        key_fetched = {getattr(x, idx_key): x for x in fetched}

    # Check if we found a data record for each name_root
    if missed := set(id_list) - key_fetched.keys():
        sys.exit(
            f"Error: Failed to fetch records for {table}.{key} in: {sorted(missed)}"
        )

    # Return objects in the order they were requested
    return list(key_fetched[x] for x in id_list)


def fetch_all(client, table, key, id_list):
    filt = DataSourceFilter(in_list={key: id_list})
    return list(client.ads.get_list(table, object_filters=filt))


def cdo_type_id(cdo):
    return f"{cdo.type}_id"


def core_data_object_to_dict(cdo):
    """Flattens a CoreDataObject to a dict"""

    # The object's ID
    flat = {cdo_type_id(cdo): cdo.id}

    # The IDs of the object's to-one related objects
    for rel_name in cdo.to_one_relationships:
        flat[f"{rel_name}_id"] = (
            getattr(rltd, "id") if (rltd := getattr(cdo, rel_name)) else None
        )

    # The object's attributes
    for k, v in cdo.attributes.items():
        if k in ("modified_at", "modified_by"):
            continue
        flat[k] = v

    return flat


def pretty_itr(obj_list, key):
    if not obj_list:
        return

    yield f"Found {bold(len(obj_list))} records:\n\n"

    cdo_key = cdo_type_id(obj_list[0])
    obj_list = list(core_data_object_to_dict(x) for x in obj_list)

    first = obj_list[0]
    max_hdr = max(len(k) for k in first)

    if key not in first and key == "id":
        if cdo_key in first:
            key = cdo_key
        else:
            sys.exit(
                f"Possible key values '{key}' or '{cdo_key}' not in: {first.keys()}"
            )

    fmt = io.StringIO("\n")
    for flat in obj_list:
        for k, v in flat.items():
            style = bold
            if v == "":
                v = "<empty string>"
                style = bold_red
            elif v is None:
                v = "null"
                style = dim
            elif k == key:
                style = bold_green

            fmt.write(f" {k:>{max_hdr}}  {style(v)}\n")
        fmt.write("\n")
        yield fmt.getvalue()


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
        if file_format and file_format == "NDJSON":
            return ids_from_ndjson_stream(key, sys.stdin)
        else:
            return parse_id_list_stream(sys.stdin)


def dim(txt):
    return click.style(txt, dim=True)


def bold_green(txt):
    return click.style(txt, bold=True, fg="green")


def bold_red(txt):
    return click.style(txt, bold=True, fg="red")


def bold(txt):
    return click.style(txt, bold=True)


def no_style(txt):
    return txt


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
