import sys

from tol.core import DataSourceFilter
from tola.goat_client import GoaTResult
from tola.ndjson import ndjson_row
from tola.tolqc_client import TolClient


def main():
    client = TolClient(tolqc_alias="tolqc-flask")
    lookup = GoaTResult.LETTER_GROUP
    for specimen in client.ads.get_list(
        "specimen", object_filters=DataSourceFilter(exact={"species.taxon_group": None})
    ):
        spcmn_id = specimen.id
        if group := lookup.get(spcmn_id[0]):
            sys.stdout.write(
                ndjson_row(
                    {
                        "species.id": specimen.species.id,
                        "taxon_group": group,
                    }
                )
            )
        else:
            sys.stderr.write(f"No taxon_group for {spcmn_id = }")


if __name__ == "__main__":
    main()
