from tola.goat_client import GoaTClient


def test_goat_fetch():
    gc = GoaTClient()
    res = gc.get_species_info(9627)
    assert res == {
        "species_id": "Vulpes vulpes",
        "tolid_prefix": "mVulVul",
        "common_name": "silver fox",
        "taxon_id": 9627,
        "taxon_family": "Canidae",
        "taxon_order": "Carnivora",
        "taxon_phylum": "Chordata",
        "taxon_group": "mammals",
        "genome_size": 2787300000,
        "chromosome_number": 34,
    }
