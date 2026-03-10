#!/usr/bin/env python3

"""
Store entries in the allocation table.  Written because `tqc add` is too slow
for large numbers of entries.
"""

import click

from tola import click_options
from tola.ndjson import get_input_objects
from tola.tolqc_client import TolClient
from tola.tqc.engine import dicts_to_core_data_objects


@click.command
@click_options.tolqc_alias
@click_options.input_files
def cli(tolqc_alias, input_files):
    client = TolClient(tolqc_alias=tolqc_alias)
    ads = client.ads
    allocations = dicts_to_core_data_objects(
        ads, "allocation", get_input_objects(input_files)
    )
    ads.upsert("allocation", allocations)


if __name__ == "__main__":
    cli()
