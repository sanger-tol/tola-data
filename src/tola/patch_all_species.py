import tola.marshals
from main.model import Species
from tola.goat_client import GoaTClient


def main():
    gc = GoaTClient()
    mrshl, _ = tola.marshals.marshal_from_command_line(
        "Patch all species with data from GOAT",
    )

    for sp in mrshl.fetch_many(Species):
        if spec_info := gc.get_species_info(sp.taxon_id):
            if sp.species_id != spec_info["species_id"]:
                print(
                    f"Species with taxon_id = '{sp.taxon_id}'"
                    f" should be named '{spec_info['species_id']}'"
                    f" not '{sp.species_id}'"
                )
            else:
                mrshl.update_or_create(Species, spec_info)

    mrshl.commit()


if __name__ == "__main__":
    main()
