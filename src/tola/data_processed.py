import sys

import click

from tol.core import DataSourceFilter

from tola import tolqc_client


@click.command
@tolqc_client.tolqc_alias
@tolqc_client.tolqc_url
@tolqc_client.api_token
@click.option(
    "--set",
    "set_processed",
    type=click.Choice(("0", "1", "null")),
    help="Set `data.processed` to this value for each of the NAME_ROOT_LIST",
)
@click.argument(
    "name_root_list",
    nargs=-1,
    required=False,
)
def cli(tolqc_alias, tolqc_url, api_token, name_root_list, set_processed):
    """
    Show or set the `processed` flag in the ToLQC `data` table.

    Each name in the NAME_ROOT_LIST should match a `data.name_root` column.

    Called without arguments it lists data rows where processed = 0

    Columns in the output are:

      - data.processed (0, 1 or null)

      - data.date (the date of the LIMS QC decision)

      - data.name_root
    """
    client = tolqc_client.TolClient(tolqc_url, api_token, tolqc_alias)
    ads = client.ads_client

    if name_root_list:
        filt = DataSourceFilter(in_list={"name_root": name_root_list})
        fetched_data = {
            x.name_root: x for x in ads.get_list("data", object_filters=filt)
        }

        # Check if we found a data record for each name_root
        if missed := set(name_root_list) - fetched_data.keys():
            sys.exit(f"Error: Failed to fetch data records named: {sorted(missed)}")

        if set_processed is not None:
            set_data_processed(ads, fetched_data, set_processed)
        for name_root in name_root_list:
            data = fetched_data[name_root]
            print_data_row(data)
    else:
        list_unproccessed_data(ads)


def set_data_processed(ads, fetched_data, set_processed):
    set_val = None if set_processed == "null" else int(set_processed)
    Obj = ads.data_object_factory
    updates = [
        Obj("data", id_=x.id, attributes={"processed": set_val})
        for x in fetched_data.values()
    ]
    ads.upsert("data", updates)
    for x in fetched_data.values():
        x.processed = set_val


def print_data_row(data):
    flag = "null" if (x := data.processed) is None else x
    print(f"{flag:<4}  {data.date.isoformat()}  {data.name_root}")


def list_unproccessed_data(ads):
    filt = DataSourceFilter(exact={"processed": 0})
    for data in ads.get_list("data", object_filters=filt):
        print_data_row(data)
