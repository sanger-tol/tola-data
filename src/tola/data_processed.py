import sys

import click

from tol.core import DataSourceFilter

from tola import tolqc_ads_client, tolqc_client


@click.command
@tolqc_client.tolqc_url
@tolqc_client.api_token
@click.option(
    "--set",
    "set_processed",
    type=click.Choice(("0", "1")),
    help="Set `data.processed` to this value for each of the NAME_ROOT_LIST",
)
@click.argument(
    "name_root_list",
    nargs=-1,
    required=False,
)
def cli(tolqc_url, api_token, name_root_list, set_processed):
    """
    Show or set the `processed` flag in the ToLQC `data` table.

    Each name in the NAME_ROOT_LIST should match a `data.name_root` column.

    Called without arguments it lists data rows where processed = 0

    Columns in the output are:
      - data.processed, where "N" = null
      - data.date (the date of the LIMS QC decision)
      - data.name_root
    """
    ads = tolqc_ads_client.tolqc_ads(tolqc_url, api_token)

    if name_root_list:
        filt = DataSourceFilter(in_list={"name_root": name_root_list})
        fetched_data = {
            x.name_root: x for x in ads.get_list("data", object_filters=filt)
        }
        exit_if_not_all_requested_fetched(name_root_list, fetched_data)
        if set_processed is not None:
            set_data_processed(ads, fetched_data, set_processed)
        for name_root in name_root_list:
            data = fetched_data[name_root]
            print_data_row(data)
    else:
        list_unproccessed_data(ads)


def set_data_processed(ads, fetched_data, set_processed):
    set_val = int(set_processed)
    Obj = ads.data_object_factory
    updates = [
        Obj("data", id_=x.id, attributes={"processed": set_val})
        for x in fetched_data.values()
    ]
    ads.upsert("data", updates)
    for x in fetched_data.values():
        x.processed = set_val


def print_data_row(data):
    flag = "N" if (x := data.processed) is None else x
    print(f"{flag}  {data.date.isoformat()}  {data.name_root}")


def exit_if_not_all_requested_fetched(wanted, fetched):
    if missed := set(wanted) - fetched.keys():
        sys.exit(f"Error: Failed to fetch data records named: {sorted(missed)!s}")


def list_unproccessed_data(ads):
    filt = DataSourceFilter(exact={"processed": 0})
    for data in ads.get_list("data", object_filters=filt):
        print_data_row(data)
