import pytest

from tola.tqc.rename import (
    ToLQCRenameError,
    build_species_rename_spec,
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


def test_build_specie_rename_spec():
    assert build_species_rename_spec(
        [
            {
                "species.id": ["A a", "B b"],
                "taxon_id": [3, 4],
            },
            {
                "scientific_name": ["C c", "D d"],
                "taxon_id": [5, 6],
            },
            {
                "species_id": ["E e", "F f"],
            },
            {  # This duplicate spec is OK
                "species.id": ["A a", "B b"],
                "taxon_id": [3, 4],
            },
        ]
    ) == {
        "A a": {"species.id": ["A a", "B b"], "taxon_id": [3, 4]},
        "C c": {"species.id": ["C c", "D d"], "taxon_id": [5, 6]},
        "E e": {"species.id": ["E e", "F f"], "taxon_id": None},
    }

    with pytest.raises(ToLQCRenameError, match=r"Found .+ when storing"):
        build_species_rename_spec(
            [
                {
                    "species.id": ["A a", "B b"],
                    "taxon_id": [3, 4],
                },
                {
                    "scientific_name": ["A a", "D d"],
                    "taxon_id": [5, 6],
                },
            ]
        )
