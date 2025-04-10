#!/usr/bin/env python3

import click
from tol.core import DataSourceFilter

from tola import click_options
from tola.tolqc_client import TolClient
from tola.tqc.engine import hash_dir


@click.command
@click_options.tolqc_alias
def cli(tolqc_alias):
    """
    Finds all specimens without a location or an epithet (subspecies and
    strains). Any linked to a species with a location use that location, else
    a location hashed on the specimen.id is linked to or created.
    """
    client = TolClient(tolqc_alias=tolqc_alias)
    ads = client.ads
    filt = DataSourceFilter(
        exact={
            "location": None,
            "epithet": None,
        }
    )

    for spcmn in ads.get_list("specimen", object_filters=filt):
        sid = spcmn.id
        if not spcmn.species:
            click.echo(f"Skipping {sid} which has no species", err=True)
            continue
        location_path = hash_dir(sid, sid)
        loc = client.fetch_or_store_one(
            "location", {"path": location_path}, key="path"
        )
        spcmn.location = loc
        ads.upsert("specimen", [spcmn])
        click.echo(f"{sid} = {location_path}", err=True)


if __name__ == "__main__":
    cli()
