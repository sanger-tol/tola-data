import logging

from tol.core import DataSourceFilter


def test_fetch(client):
    rspns = client.json_get("data/project", {"filter": {"exact": {"study_id": 5901}}})
    assert len(rspns["data"]) == 1
    assert rspns["data"][0]["attributes"] == {
        "description": "DTOL_Darwin Tree of Life",
        "hierarchy_name": "darwin/{}",
        "study_id": 5901,
    }


def test_fetch_ads(ads):
    filt = DataSourceFilter()
    filt.exact = {"study_id": 5901}
    (darwin,) = ads.get_list("project", object_filters=filt)
    assert darwin is not None
    assert darwin.study_id == 5901


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
