import sys

from tol.core import DataSourceFilter

from tola.goat_client import GoaTClient
from tola.ndjson import ndjson_row
from tola.tolqc_client import TolClient


def main():
    client = TolClient(tolqc_alias="tolqc-flask")
    gc = GoaTClient()
    for species in client.ads.get_list(
        "species", object_filters=DataSourceFilter(exact={"family_taxon_id": None})
    ):
        if spec_info := gc.get_species_info(species.taxon_id):
            sys.stdout.write(
                ndjson_row(
                    {
                        "species.id": species.id,
                        "family_taxon_id": spec_info["family_taxon_id"],
                    }
                )
            )


if __name__ == "__main__":
    main()
