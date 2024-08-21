#!/usr/bin/env python3

from sys import stderr, stdout

from tol.core import DataSourceFilter
from tola.goat_client import GoaTClient
from tola.ndjson import ndjson_row
from tola.tolqc_client import TolClient


def main():
    client = TolClient(tolqc_alias="tolqc-flask-ro")
    gc = GoaTClient()
    for species in client.ads.get_list(
        "species", object_filters=DataSourceFilter(exact={"tolid_prefix": None})
    ):
        info = gc.get_species_info(species.taxon_id)
        if not info:
            stderr.write(f"No GoaT info for {species.id} {species.taxon_id}\n")
            continue
        diff = {}
        if not info["tolid_prefix"]:
            stderr.write(f"No tolid_prefix for {species.id} {species.taxon_id}\n")
            continue
        for fld, val in info.items():
            if fld == "species_id":
                continue
            if val != getattr(species, fld):
                diff[fld] = val
        if diff:
            stdout.write(ndjson_row({"species.id": species.id, **diff}))


if __name__ == "__main__":
    main()
