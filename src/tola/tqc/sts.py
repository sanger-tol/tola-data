import sys

import click
from tol.core import DataSourceFilter
from tol.sources.portal import portal

from tola import click_options
from tola.ndjson import ndjson_row
from tola.pretty import colour_pager
from tola.terminal import pretty_dict_itr
from tola.tolqc_client import uc_munge
from tola.tqc.engine import core_data_object_to_dict


@click.command()
@click.pass_context
@click.option(
    "--info",
    "show_info",
    flag_value=True,
    default=False,
    help="Only show the info fetched from the ToL Portal. Don't diff against ToLQC",
)
@click.option(
    "--all-fields",
    flag_value=True,
    default=False,
    help="Show all the fields from the Portal's sample page",
)
@click.option(
    "--auto-populate",
    flag_value=True,
    default=False,
    help="""
      Query the ToLQC `specimen` table for any empty `supplied_name` or
      `sex.id` fields, and fill them in using the ToL Portal's `sample` tab.
      """,
)
@click.argument(
    "specimen-names",
    nargs=-1,
)
@click_options.apply_flag
def sts_specimen(ctx, specimen_names, show_info, all_fields, auto_populate, apply_flag):
    """
    Fetches fields required for the `specimen` table from STS values found in
    the ToL Portal.
    """

    client = ctx.obj
    ads = client.ads

    if all_fields:
        show_info = True

    if auto_populate and specimen_names:
        sys.exit(
            ctx.get_help()
            + "\n\n"
            + "Error: Cannot specify both --auto-populate and SPECIMEN_NAMES"
        )

    if auto_populate:
        patches = fetch_specimen_info_where_fields_are_null(ads)
        specimen_names = list(patches)
    elif specimen_names:
        patches = fetch_specimen_info_for_specimens(ads, specimen_names)
    else:
        return

    if show_info:
        if all_fields:
            sts_info = [
                core_data_object_to_dict(x)
                for x in fetch_sts_data_itr_from_portal(specimen_names)
            ]
        else:
            sts_info = fetch_sts_info(client, specimen_names)

        if sys.stdout.isatty():
            key = "sts_tolid.id" if all_fields else "specimen.id"
            colour_pager(pretty_dict_itr(sts_info, key))
        else:
            for info in sts_info:
                sys.stdout.write(ndjson_row(info))
        return

    updates = []
    for info in fetch_sts_info(client, specimen_names):
        sid = info["specimen.id"]
        dbv = patches[sid]
        update_flag = False
        chng = {"specimen.id": sid}
        for fld in "supplied_name", "sex.id":
            if dbv[fld] is None and (val := info[fld]):
                update_flag = True
                chng[fld] = val
        if update_flag:
            updates.append(chng)

    if updates:
        if sys.stdout.isatty():
            colour_pager(pretty_dict_itr(updates, "specimen.id"))
        else:
            for upd in updates:
                sys.stdout.write(ndjson_row(upd))


def fetch_specimen_info_where_fields_are_null(ads):
    filt = DataSourceFilter(
        or_={
            "supplied_name": {"eq": {"value": None}},
            "sex.id": {"eq": {"value": None}},
        }
    )
    return fetch_specimen_info_by_filter(ads, filt)


def fetch_specimen_info_for_specimens(ads, specimen_names):
    filt = DataSourceFilter(in_list={"id": specimen_names})
    return fetch_specimen_info_by_filter(ads, filt)


def fetch_specimen_info_by_filter(ads, filt):
    itr = ads.get_list("specimen", object_filters=filt)
    fields = ["specimen.id", "supplied_name", "sex.id"]
    found = [obj_to_dict(fields, x) for x in itr]
    return {x["specimen.id"]: x for x in found}


def fetch_sts_info(client, specimen_names):
    fields_wanted = [
        "sts_tolid.id",
        "sts_specimen.id",
        "sts_sex",
    ]
    sex_tbl = client.sex_table

    # Treat these uninformative values as NULL
    null_values = {"NOT_COLLECTED", "NOT_PROVIDED"}

    found = []
    for cdo in fetch_sts_data_itr_from_portal(specimen_names):
        sts = obj_to_dict(fields_wanted, cdo)
        out = {
            "specimen.id": sts["sts_tolid.id"],
            "supplied_name": sts["sts_specimen.id"],
        }
        sex = None
        if sts_sex := sts["sts_sex"]:
            key = uc_munge(sts_sex)
            sex = None if key in null_values else sex_tbl.get(key, sts_sex)
        out["sex.id"] = sex

        found.append(out)

    return de_duplicate_dicts("specimen.id", found)


def fetch_sts_data_itr_from_portal(specimen_names):
    prtl = portal()
    prtl.page_size = 200

    filt = DataSourceFilter()
    filt.and_ = {
        "sts_tolid.id": {"in_list": {"value": specimen_names}},
        # Excludes records which haven't been exported from STS, so won't have
        # gone to the lab:
        "sts_ep_exported": {"eq": {"value": True}},
    }
    return prtl.get_list("sample", object_filters=filt)


def obj_to_dict(fields, cdo):
    """
    Extracts all the listed `fields` from CoreDataObject `cdo`, returning them
    in a `dict`.
    """
    dct = {}
    for fld in fields:
        if fld.endswith(".id"):
            relname = fld[:-3]
            if relname == cdo.type:
                dct[fld] = cdo.id
            else:
                rltd = getattr(cdo, relname)
                dct[fld] = rltd.id if rltd else None
        else:
            dct[fld] = getattr(cdo, fld)

    return dct


def de_duplicate_dicts(key, dict_list):
    """
    Removes duplicate dicts identified by `key` from `dict_list` if all the
    fields of the duplicates match. If any field in a set of duplicates does
    not match, a warning is logged, and all the copies are removed from the
    output.
    """
    sngl = {}
    skip = set()
    for d in dict_list:
        k = d[key]
        if k in skip:
            continue
        if xst := sngl.get(k):
            if xst != d:
                click.echo(f"Mismatched responses for '{key}':\n{d}\n{xst}")
                sngl.pop(k)
                skip.add(k)
        else:
            sngl[k] = d

    return list(sngl.values())
