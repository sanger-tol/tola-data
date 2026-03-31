from tola.tqc.query_parser import QueryParser


def test_query_parser():
    params = [
        "taxon_family=Canidae",
        "species!=Vulpes vulpes",
        "date<2026-01-28",
        "ploidy>2",
        "chromosome_number<=8",
        "read_length_n50>=50000",
        "processed=null",
        "common_name!=null",
        "library_type_id%HiFi",
        "specimen.species.id!%corax",
    ]
    qp = QueryParser(params)
    assert qp.filter_dict() == {
        "taxon_family": {
            "eq": {
                "value": "Canidae",
            }
        },
        "species": {
            "eq": {
                "value": "Vulpes vulpes",
                "negate": True,
            }
        },
        "date": {
            "lt": {
                "value": "2026-01-28",
            }
        },
        "ploidy": {
            "gt": {
                "value": "2",
            }
        },
        "chromosome_number": {
            "lte": {
                "value": "8",
            }
        },
        "read_length_n50": {
            "gte": {
                "value": "50000",
            }
        },
        "processed": {
            "exists": {
                "negate": True,
            }
        },
        "common_name": {
            "exists": {},
        },
        "library_type_id": {
            "contains": {
                "value": "HiFi",
            },
        },
        "specimen.species.id": {
            "contains": {
                "value": "corax",
                "negate": True,
            },
        },
    }
