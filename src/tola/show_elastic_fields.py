import logging
import sys

from tol.api_client import ApiDataSource
from tol.core import DataSourceFilter

log = logging.getLogger(__name__)


def main(args):
    logging.basicConfig(level=logging.INFO)
    src = ApiDataSource({"url": "https://portal.tol.sanger.ac.uk/api/v1", "key": ""})
    filt = DataSourceFilter()

    if "--TOLP-5914" in args:
        tries = 100
        attempt_ok = 0
        for attempt in range(tries):
            info(f"Starting attempt {attempt + 1}")
            filt.exact = {
                "mlwh_run_status": "qc complete",
                "mlwh_study_id": "5901",  # Darwin Tree of Life
            }
            run_data = src.get_list(
                "run_data",
                object_filters=filt,
                sort_by="mlwh_instrument_model",
            )
            n = 0
            id_set = set()
            for rd in run_data:
                n += 1
                id_set.add(rd.id)
            if n == len(id_set):
                attempt_ok += 1
            else:
                info(f"Got {n} items {len(id_set)} unique")

        if attempt_ok == tries:
            info(f"Correct number of entries in all {attempt_ok} fetches")
        else:
            info(f"Wrong number of entries {tries - attempt_ok} fetches out of {tries}")

        return 0

    # filt.exact = {"tolqc_run_id": None}
    filt.exact = {
        "mlwh_run_status": "qc complete",
        # "mlwh_platform_type": "pacbio",
        # "mlwh_study_id": "5308",  # Malawi-Tanganyika convergent cichlid sequencing
        # "mlwh_study_id": "5113",  # Black soldier fly
        # "mlwh_study_id": "5485",  # Helmithns R&D
        # "mlwh_study_id": "7112",  # Heliconius Reference Genome Heliconius
        # "mlwh_study_id": "5884",  # Darwin Lepidoptera
        # "mlwh_study_id": "5853",  # Darwin Plants
        "mlwh_study_id": "5901",  # Darwin Tree of Life
    }
    # filt.contains = {}
    # filt.in_list = {"mlwh_study_id": study_ids}

    run_data = src.get_list("run_data", object_filters=filt, sort_by="mlwh_run_id")
    # run_data = src.get_list("run_data", object_filters=filt, sort_by="mlwh_run_id")
    # run_data = src.get_list("run_data", object_filters=filt, sort_by="mlwh_instrument_model")

    n = 0
    id_set = set()
    for rd in run_data:
        n += 1
        id_set.add(rd.id)
        info(
            f"id = {rd.id}",
            # f"type = {rd.type}",
        )
    info(f"Got {n} items {len(id_set)} unique")
    ### Only seems to be returning multiples of 10 (or 20?) if > 1 page results


def info(*args):
    for item in args:
        log.info(item)


if __name__ == "__main__":
    main(sys.argv[1:])
