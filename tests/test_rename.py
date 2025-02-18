import pytest

from tola.tqc.rename import (
    ToLQCRenameError,
    build_spec_dict,
    get_rename_field,
)


def test_get_rename_field():
    assert get_rename_field(("a", "b", "c"), {"a": ["new", "old"]}) == ["new", "old"]
    with pytest.raises(
        ToLQCRenameError, match=r"Failed to find any of .+ in JSON object:"
    ):
        get_rename_field(("a",), {"b": ["new", "old"]})
    with pytest.raises(
        ToLQCRenameError, match=r"Value under 'a' key is not a list in JSON object:"
    ):
        get_rename_field(("a",), {"a": {"j": 10}})
    assert get_rename_field(("a",), {"b": ["new", "old"]}, maybe=True) is None
    assert get_rename_field(("a",), {"a": None}, maybe=True) is None
    with pytest.raises(
        ToLQCRenameError, match=r"Expecting a rename but the 2 values .+ match"
    ):
        assert get_rename_field(("a",), {"a": ["new", "new"]})


def test_build_species_rename_spec():
    assert build_spec_dict(
        "species",
        [
            {
                "species.id": ["B b", "A a"],
                "taxon_id": [4, 3],
            },
            {
                "scientific_name": ["D d", "C c"],
                "taxon_id": [6, 5],
            },
            {
                "species_id": ["F f", "E e"],
            },
            {  # This duplicate spec is OK
                "species.id": ["B b", "A a"],
                "taxon_id": [4, 3],
            },
        ],
    ) == {
        "A a": {"species.id": ["B b", "A a"], "taxon_id": [4, 3]},
        "C c": {"species.id": ["D d", "C c"], "taxon_id": [6, 5]},
        "E e": {"species.id": ["F f", "E e"], "taxon_id": None},
    }

    with pytest.raises(ToLQCRenameError, match=r"Found .+ when building"):
        build_spec_dict(
            "species",
            [
                {
                    "species.id": ["B b", "A a"],
                    "taxon_id": [4, 3],
                },
                {
                    "scientific_name": ["D d", "A a"],
                    "taxon_id": [6, 5],
                },
            ],
        )
