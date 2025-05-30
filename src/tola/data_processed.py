import sys

import click
from tol.core import DataSourceFilter

from tola import click_options, tolqc_client
from tola.tqc.engine import fetch_all


@click.command
@click_options.tolqc_alias
@click_options.tolqc_url
@click_options.api_token
@click.option(
    "--set",
    "set_processed",
    type=click.Choice(("0", "1", "null")),
    help="Set `data.processed` to this value for each of the NAME_LIST",
)
@click.argument(
    "data-id-list",
    nargs=-1,
    required=False,
)
def cli(tolqc_alias, tolqc_url, api_token, data_id_list, set_processed):
    """
    Show or set the `processed` flag in the ToLQC `data` table.

    Each name in the DATA_ID_LIST should match a `data.data_id` column.

    Called without arguments it lists data rows where processed = 0

    Columns in the output are:

      - data.processed (0, 1 or null)

      - data.date (the date of the LIMS QC decision)

      - data.data_id
    """
    client = tolqc_client.TolClient(tolqc_url, api_token, tolqc_alias)
    ads = client.ads

    if data_id_list:
        fetched_data = {
            x.id: x for x in fetch_all(client, "data", "data.id", data_id_list)
        }

        # Check if we found a data record for each name
        if missed := set(data_id_list) - fetched_data.keys():
            sys.exit(f"Error: Failed to fetch data records named: {sorted(missed)}")

        if set_processed is not None:
            set_data_processed(ads, fetched_data, set_processed)
        for name in data_id_list:
            data = fetched_data[name]
            print_data_row(data)
    else:
        list_unproccessed_data(ads)


def set_data_processed(ads, fetched_data, set_processed):
    set_val = None if set_processed == "null" else int(set_processed)
    Obj = ads.data_object_factory  # noqa: N806
    updates = [
        Obj("data", id_=x.id, attributes={"processed": set_val})
        for x in fetched_data.values()
    ]
    ads.upsert("data", updates)
    for x in fetched_data.values():
        x.processed = set_val


def print_data_row(data):
    flag = "null" if (x := data.processed) is None else x
    date_str = d.isoformat() if (d := data.date) else ""
    print(f"{flag:<4}  {date_str:25}  {data.id}")


def list_unproccessed_data(ads):
    filt = DataSourceFilter(exact={"processed": 0})
    for data in ads.get_list("data", object_filters=filt):
        print_data_row(data)
