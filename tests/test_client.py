import logging

import pytest
from tol.core import DataSourceFilter

from tola.tolqc_client import TolClientError


def test_fetch_study(client):
    rspns = client.json_get("data/study", {"filter": {"exact": {"study_id": 5901}}})
    assert len(rspns["data"]) == 1
    assert rspns["data"][0]["attributes"] == {
        "name": "DTOL_Darwin Tree of Life",
        "auto_sync": True,
        "modified_at": None,
    }


def test_fetch_all_study_ids(client):
    id_list = client.list_auto_sync_study_ids()
    assert len(id_list) > 1
    for x in id_list:
        assert isinstance(x, int)


def test_fetch_ads(ads):
    filt = DataSourceFilter()
    filt.exact = {"study_id": 5901}
    (darwin,) = ads.get_list("study", object_filters=filt)
    assert darwin is not None
    assert darwin.id == "5901"


def test_upsert(ads):
    species = ads.data_object_factory(
        "species",
        id_="Andrena fulva",
        attributes={"common_name": "tawny mining bee"},
    )

    (res,) = ads.upsert(species.type, [species])
    logging.info(res.attributes)
    assert res


def test_fetch_unfilled_species(ads):
    filt = DataSourceFilter()
    filt.exact = {
        "taxon_family": None,
        "taxon_order": None,
        "taxon_phylum": None,
        "taxon_group": None,
    }
    unfilled = list(ads.get_list("species", object_filters=filt))
    assert len(unfilled)


def test_fetch_or_store_one(client):
    tbl = "library_type"
    spec = {
        "library_type.id": "ACME Corp Genome Sequencer",
        "default_category": "genomic_data",
    }
    with pytest.raises(TolClientError, match=r"Multiple matches"):
        client.fetch_or_store_one(tbl, spec, key="default_category")
    lib_typ = client.fetch_or_store_one(tbl, spec)
    assert lib_typ.id == spec["library_type.id"]
    client.ads.delete(tbl, [lib_typ.id])
