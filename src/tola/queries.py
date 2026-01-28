import re

from tol.core import DataSourceFilter

from tola.tolqc_client import TolClient


class TolQueryError(Exception):
    """
    Error encountered when querying the ToLQC database, usually due to
    unexpected or missing data.
    """


def fetch_specimen_ploidy(client: TolClient, dataset_id: str) -> int | None:
    """
    Given a dataset.id, returns the ploidy from the attached specimen, recast
    as an int, or None if `specimen.ploidy` is null.

    Throws a `TolQueryError` if there is not one and only one specimen related
    to the dataset.
    """
    specimen_list = list(
        client.ads.get_list(
            "specimen",
            object_filters=DataSourceFilter(
                exact={"samples.data.dataset_assn.dataset.id": dataset_id}
            ),
        )
    )
    if len(specimen_list) == 1:
        ploidy = specimen_list[0].ploidy
        if ploidy is None:
            return None
        else:
            if m := re.search(r"\d+", ploidy):
                return int(m.group(0))
            return None

    msg = "Fetching ploidy for dataset.id {dataset_id!r}"
    if specimen_list:
        found_ids = [s.id for s in specimen_list]
        msg += f" found multiple specimens: {found_ids!r}"
    else:
        msg += " failed to find a specimen"
    raise TolQueryError(msg)
