import click

from tola import click_options
from tola.goat_client import GoaTClient
from tola.tqc.engine import hash_dir, id_iterator
from tola.tqc.upsert import TableUpserter


@click.command()
@click.pass_context
@click.option(
    "--key",
    default="taxon_id",
    show_default=True,
    help=("Column name containing the NCBI taxon ID"),
)
@click_options.file
@click_options.file_format
@click_options.apply_flag
@click_options.id_list
def upsert_species(ctx, key, file_list, file_format, id_list, apply_flag):
    """
    Add new rows to the species table and populate location if strain is not set.

    ID_LIST is a list of taxon_id values to operate on, which can additionally
    be provided in --file arugments, or alternatively piped to STDIN.
    """

    # Gather info for each of the species from GoaT
    gc = GoaTClient()
    species_info = []
    seen_taxon = set()
    for taxon_id in id_iterator(key, id_list, file_list, file_format):
        if taxon_id in seen_taxon:
            continue
        seen_taxon.add(taxon_id)
        if sp_info := gc.get_species_info(taxon_id):
            species_id = sp_info.pop("species_id")
            species_info.append({"species.id": species_id, **sp_info})

    # Build the hashed paths for each species
    loc_info = []
    path_sp_info = {}
    for sp_info in species_info:
        taxon_id = sp_info["taxon_id"]
        sci_name = sp_info["species.id"]
        if taxon_id and sci_name:
            path = hash_dir(taxon_id, sci_name)
            path_sp_info[path] = sp_info
            loc_info.append({"path": path})

    # Upload the paths to the location table and record the auto-incremented
    # location_id in each species info
    client = ctx.obj
    ups = TableUpserter(client)
    ups.build_table_upserts("location", loc_info, key="path")
    if apply_flag:
        for cdo in ups.apply_upserts():
            path = cdo.path
            if sp_info := path_sp_info.get(path):
                sp_info["location.id"] = cdo.id
            else:
                msg = "Missing species info for path {path!r}"
                raise ValueError(msg)

    # Prepare and apply the updates if the `apply_flag` is set
    ups.build_table_upserts("species", species_info)
    if apply_flag:
        ups.apply_upserts()

    # Show the results
    ups.page_results(apply_flag)
