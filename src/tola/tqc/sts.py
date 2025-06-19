import sys

import click
from tol.core import DataSourceFilter
from tol.sources.portal import portal

from tola import click_options
from tola.ndjson import ndjson_row
from tola.pretty import colour_pager
from tola.terminal import pretty_dict_itr
from tola.tolqc_client import uc_munge
from tola.tqc.engine import core_data_object_to_dict, id_iterator
from tola.tqc.upsert import TableUpserter


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
@click_options.apply_flag
@click.option(
    "--key",
    default="specimen.id",
    show_default=True,
    help="Name of field containing specimen IDs in ND-JSON input files",
)
@click_options.file
@click_options.file_format
@click.argument(
    "specimen-names",
    nargs=-1,
)
def sts_specimen(
    ctx,
    show_info,
    all_fields,
    auto_populate,
    apply_flag,
    key,
    file_list,
    file_format,
    specimen_names,
):
    """
    Fetches fields required for the `specimen` table from STS values found in
    the ToL Portal.
    """

    if key == "id":
        key = "specimen.id"

    specimen_names = list(id_iterator(key, specimen_names, file_list, file_format))

    client = ctx.obj

    if all_fields:
        show_info = True

    if auto_populate and specimen_names:
        sys.exit(
            ctx.get_help()
            + "\n\n"
            + "Error: Cannot specify both --auto-populate and SPECIMEN_NAMES"
        )

    if auto_populate:
        patches = fetch_specimen_info_where_fields_are_null(client)
        specimen_names = list(patches)
    elif specimen_names:
        patches = fetch_specimen_info_for_specimens(client, specimen_names)
    else:
        return

    if show_info:
        show_sts_info(client, all_fields, specimen_names)
    else:
        if upsrtr := update_specimen_fields(
            client, specimen_names, patches, apply_flag
        ):
            upsrtr.page_results(apply_flag)


def update_specimen_fields(client, specimen_names, patches, apply_flag=False):
    updates = []
    acc_updates = []
    for info in fetch_sts_info(client, specimen_names):
        sid = info["specimen.id"]
        dbv = patches[sid]
        update_flag = False
        chng = {"specimen.id": sid}
        for fld in "supplied_name", "accession.id", "sex.id":
            if dbv[fld] is None and (val := info[fld]):
                update_flag = True
                chng[fld] = val
        # if (val := info["supplied_name"]) and val != dbv["supplied_name"]:
        #     update_flag = True
        #     chng["supplied_name"] = val
        if update_flag:
            updates.append(chng)
            if acc := chng.get("accession.id"):
                acc_updates.append(
                    {
                        "accession.id": acc,
                        "accession_type.id": "BioSpecimen",
                    }
                )

    if updates:
        upsrtr = TableUpserter(client)
        upsrtr.build_table_upserts("accession", acc_updates)
        upsrtr.build_table_upserts("specimen", updates)
        if apply_flag:
            upsrtr.apply_upserts()
        return upsrtr


def show_sts_info(client, all_fields, specimen_names):
    if all_fields:
        sts_info = [
            core_data_object_to_dict(x)
            for x in fetch_sts_data_itr_from_portal(client, specimen_names)
        ]
    else:
        sts_info = fetch_sts_info(client, specimen_names)

    if sys.stdout.isatty():
        key = "sts_tolid.id" if all_fields else "specimen.id"
        colour_pager(pretty_dict_itr(sts_info, key))
    else:
        for info in sts_info:
            sys.stdout.write(ndjson_row(info))


def fetch_specimen_info_where_fields_are_null(client):
    filt = DataSourceFilter()
    filt.or_ = {
        "supplied_name": {"eq": {"value": None}},
        "accession.id": {"eq": {"value": None}},
        "sex.id": {"eq": {"value": None}},
    }
    return fetch_specimen_info_by_filter(client, filt)


def fetch_specimen_info_for_specimens(client, specimen_names):
    ads = client.ads
    for page in client.pages(specimen_names):
        filt = DataSourceFilter(in_list={"id": page})
        yield from fetch_specimen_info_by_filter(ads, filt)


def fetch_specimen_info_by_filter(client, filt):
    itr = client.ads.get_list("specimen", object_filters=filt)
    fields = ["specimen.id", "supplied_name", "accession.id", "sex.id"]
    found = [obj_to_dict(fields, x) for x in itr]
    return {x["specimen.id"]: x for x in found}


def fetch_sts_info(client, specimen_names):
    fields_wanted = [
        "sts_tolid.id",
        "sts_specimen.id",
        "sts_biospecimen_accession",
        "sts_sample_same_as",
        "sts_sex",
    ]
    sex_tbl = client.sex_table

    found = []
    for cdo in fetch_sts_data_itr_from_portal(client, specimen_names):
        sts = obj_to_dict(fields_wanted, cdo)
        out = {
            "specimen.id": sts["sts_tolid.id"],
            "supplied_name": sts["sts_specimen.id"],
            "accession.id": (
                sts["sts_biospecimen_accession"] or sts["sts_sample_same_as"]
            ),
        }
        sex = None
        if sts_sex := sts["sts_sex"]:
            key = uc_munge(sts_sex)
            sex = sex_tbl.get(key, sts_sex)
        out["sex.id"] = sex

        found.append(out)

    return de_duplicate_dicts("specimen.id", found)


def fetch_sts_data_itr_from_portal(client, specimen_names):
    prtl = portal()
    prtl.page_size = 200

    for page in client.pages(specimen_names):
        filt = DataSourceFilter(
            and_={
                "sts_tolid.id": {"in_list": {"value": page}},
            }
        )
        yield from prtl.get_list("sample", object_filters=filt)


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
    for flat in dict_list:
        oid = flat[key]
        if oid in skip:
            continue
        if xst := sngl.get(oid):
            if xst != flat:
                click.echo(
                    f"\nMismatched responses for '{oid}':\n"
                    f"  {ndjson_row(xst)}  {ndjson_row(flat)}",
                    nl=False,
                    err=True,
                )
                sngl.pop(oid)
                skip.add(oid)
        else:
            sngl[oid] = flat

    return list(sngl.values())
