import asyncio
import logging
import re
import sys
from datetime import datetime
from hashlib import md5
from pathlib import Path

from partisan.irods import DataObject
from tol.core import DataSourceFilter

from tola.ndjson import (
    get_input_objects,
    parse_ndjson_stream,
)
from tola.pretty import bold

log = logging.getLogger(__name__)


def table_key(table, key):
    """
    Turn the key into "id" if it is in the flattened representation of the
    table's primary key, e.g. "species.id"
    """
    return "id" if key == f"{table}.id" else key


def cdo_type_id(cdo):
    return f"{cdo.type}.id"


def obj_rel_name(key):
    return key[:-3] if key.endswith(".id") else None


def parse_datetime_fields(field_list, input_obj):
    """
    Parsing the date / time fields into datetime objects ensures that time
    zones will be added when stringified by the `ndjson_row()` function.
    """
    for obj in input_obj:
        for fld in field_list:
            if x := obj.get(fld):
                dt = datetime.fromisoformat(x)
                obj[fld] = dt


def input_objects_or_exit(ctx, input_files):
    if not input_files and sys.stdin.isatty():
        err = "Error: " + bold("Missing INPUT_FILES arguments or STDIN input")
        sys.exit(ctx.get_help() + "\n\n" + err)

    input_obj = get_input_objects(input_files)
    if not input_obj:
        sys.exit("No input objects")

    return input_obj


def hierarchy_name(text):
    """
    Turns strings of non-word characters into underscores, and trims them from
    either end of the returned string.
    """
    return re.sub(r"\W+", "_", text).strip("_")


def hash_dir(hash_me, sci_name):
    dir_name = hierarchy_name(sci_name)
    hash_prefix = md5(str(hash_me).encode()).hexdigest()[:6]  # noqa: S324
    return "/".join((*hash_prefix, dir_name))


def key_list_search(client, table, key, key_id_list):
    db_obj_found = {}
    if key_id_list:
        search_key = table_key(table, key)
        obj_rel = obj_rel_name(search_key)

        for req_list in client.pages(key_id_list):
            filt = DataSourceFilter(in_list={search_key: req_list})
            for cdo in client.ads_ro.get_list(table, object_filters=filt):
                val = getattr(cdo, obj_rel).id if obj_rel else getattr(cdo, search_key)
                if not val:
                    sys.exit(f"No such key '{search_key}' in {cdo!r}")
                if db_obj_found.get(val):
                    sys.exit(
                        f"More than one row in '{table}' table"
                        f" with '{search_key}' = '{val}'"
                    )
                else:
                    db_obj_found[val] = cdo

    return db_obj_found


def fetch_list_or_exit(client, table, key, id_list):
    """
    Fetches all the records for `id_list` in the same order, or exits with an
    error.
    """

    key_fetched = key_list_search(client, table, key, id_list)

    # Check if we found a data record for each name
    if missed := set(id_list) - key_fetched.keys():
        sys.exit(
            "Error: Failed to fetch records from "
            f"{table} for {key} in: {sorted(missed)}"
        )

    # Return objects in the order they were requested
    return [key_fetched[x] for x in id_list]


def fetch_all(client, table, key, id_list, show_modified=False):
    key = table_key(table, key)

    # Not using get_by_ids():
    #
    #    species = client.ads.get_by_ids("species", id_list)
    #
    # because it does a separate GET for each ID

    modified = __req_fields_from_show_modified_flag(show_modified)

    if id_list:
        fetched = []
        for req_list in client.pages(id_list):
            filt = DataSourceFilter(in_list={key: req_list})
            fetched.extend(
                list(client.ads_ro.get_list(table, object_filters=filt, **modified))
            )
        return fetched
    else:
        return list(client.ads_ro.get_list(table, **modified))


def async_fetch_all_itr(client, table, key, id_list, show_modified=False):
    key = table_key(table, key)
    modified = __req_fields_from_show_modified_flag(show_modified)

    if id_list:
        return async_query_id_list_pager(client, table, key, id_list, modified)
    return async_cursor_list_pager(client, table, modified)


async def async_query_id_list_pager(client, table, key, id_list, modified):
    loop = asyncio.get_running_loop()
    ads_ro = client.ads_ro
    for req_list in client.pages(id_list):
        filt = DataSourceFilter(in_list={key: req_list})

        def get_page(filt=filt):
            return ads_ro.get_list(table, object_filters=filt, **modified)

        yield await loop.run_in_executor(None, get_page)


async def async_cursor_list_pager(client, table, modified):
    loop = asyncio.get_running_loop()
    ads_ro = client.ads_ro

    # Should check that we can use cursor fetching, but cannot call private
    # method `__can_cursor()` on `ApiDataSource`
    search_after = None
    page_size = client.page_size

    while True:

        def page_fetch(search_after=search_after):
            return ads_ro.get_cursor_page(
                table,
                page_size=page_size,
                search_after=search_after,
                **modified,
            )

        obj_itr, search_after = await loop.run_in_executor(None, page_fetch)
        fetched = list(obj_itr)
        if fetched:
            yield fetched
        if len(fetched) < page_size:
            break


def __req_fields_from_show_modified_flag(show_modified=False):
    return {"requested_fields": ["modified_user"]} if show_modified else {}


def core_data_object_to_dict(cdo, show_modified=False):
    """Flattens a CoreDataObject to a dict"""

    # The object's ID
    flat = {cdo_type_id(cdo): cdo.id}

    # Save LogBase fields
    modfd = {}

    # The IDs of the object's to-one related objects
    for rel_name in cdo.to_one_relationships:
        log.debug(f"{cdo = }")
        rltd = getattr(cdo, rel_name)
        if rel_name == "modified_user":
            if show_modified:
                modfd["modified_by"] = rltd.name if rltd else None
        else:
            flat[f"{rel_name}.id"] = rltd.id if rltd else None

    # The object's attributes
    for k, v in cdo.attributes.items():
        if k == "modified_at":
            if show_modified:
                modfd[k] = v
            continue
        flat[k] = v

    if show_modified:
        for attr in ("modified_by", "modified_at"):
            flat[attr] = modfd.get(attr)

    return flat


def dicts_to_core_data_objects(ads, table, flat_list):
    """Turns flattened dicts back into CoreDataObjects"""

    rel_conf = ads.relationship_config.get(table)
    obj_factory = ads.data_object_factory

    cdo_out = []
    for flat in flat_list:
        id_ = None
        attr = {}
        to_one = {}
        for key, val in flat.items():
            if rn := obj_rel_name(key):
                if rn == table:
                    id_ = val
                elif to_one_tbl := rel_conf.to_one.get(rn):
                    to_one[rn] = obj_factory(to_one_tbl, id_=val)
                elif to_many_tbl := rel_conf.to_many.get(rn):
                    msg = (
                        f"to-many relationships not implemented"
                        f" ('{rn}' to '{to_many_tbl}')"
                    )
                    raise ValueError(msg)
                else:
                    msg = f"No such relationship '{rn}'"
                    raise ValueError(msg)
            else:
                attr[key] = val

        cdo_out.append(
            obj_factory(
                table,
                id_=id_,
                # attributes=attr if attr else None,
                # to_one=to_one if to_one else None,
                attributes=attr,
                to_one=to_one,
            )
        )

    return cdo_out


def id_iterator(key, id_list=None, file_list=None, file_format=None):
    if id_list:
        yield from id_list
        return

    if file_list:
        for file in file_list:
            fmt = file_format or guess_file_type(file)
            with file.open() as fh:
                if fmt == "TXT":
                    for oid in parse_id_list_stream(fh):
                        yield oid
                else:
                    for oid in ids_from_ndjson_stream(key, fh):
                        yield oid
    elif not sys.stdin.isatty():
        # No IDs or files given on command line, and input is not attached to
        # a terminal, so read from STDIN.
        if file_format == "TXT":
            for oid in parse_id_list_stream(sys.stdin):
                yield oid
        else:
            for oid in ids_from_ndjson_stream(key, sys.stdin):
                yield oid


def irods_path_dataobject(txt):
    irods_obj = None
    if txt.startswith("irods:"):
        path = Path(txt[6:])
        irods_obj = DataObject(path)
    else:
        path = Path(txt)

    return path, irods_obj


def update_file_size_and_md5_if_missing(client, spec, irods_obj):
    if not (spec.get("size_bytes") is None or spec.get("md5") is None):
        return

    if not irods_obj.exists():
        log.warning(f"No such iRODS file: '{irods_obj}'")
        return
    size_bytes = irods_obj.size()
    md5 = irods_obj.checksum()
    if not (size_bytes and md5):
        log.warning(
            f"Missing iRODS data: Got size = {size_bytes!r}"
            f" and checksum = {md5!r} for '{irods_obj}'"
        )
        return

    file_id = spec.get("file.id") or spec.get("file_id")
    if not file_id:
        msg = f"Missing 'file.id' or 'file_id' field in: {spec}"
        raise ValueError(msg)
    cdo = client.build_cdo(
        "file",
        file_id,
        {
            "size_bytes": size_bytes,
            "md5": md5,
        },
    )
    client.ads.upsert("file", [cdo])


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
