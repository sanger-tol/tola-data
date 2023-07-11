import os

from tol.api_client import ApiDataSource
from tol.core import DataSourceFilter


def main():
    tolqc = ApiDataSource(
        {
            "url": "https://qc.tol.sanger.ac.uk/api/v1",
            "key": os.getenv("TOLQC_API_KEY"),
        }
    )
    filt = DataSourceFilter()
    filt.exact = {"name": "22052_1_3"}
    data = tolqc.get_list(
        "data",
        object_filters=filt,
    )
    for d in data:
        # d.auto_qc = 0
        print(f"name = '{d.name}'")
        # for attr in ("creator", "last_modifier", "last_modified_at", "created_at"):
        #     delattr(d, attr)
        tolqc.update(d)
        # tolqc.create(d)
    # foo = [
    #     {"detail": "Unknown field.", "source": {"pointer": "/data/relationships/creator/data"}},
    #     {"detail": "Unknown field.", "source": {"pointer": "/data/relationships/last_modifier/data"}},
    #     {"detail": "Unknown field.", "source": {"pointer": "/data/attributes/last_modified_at"}},
    #     {"detail": "Unknown field.", "source": {"pointer": "/data/attributes/created_at"}},
    # ]


if __name__ == "__main__":
    main()
