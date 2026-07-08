import sys

import click

from tola import click_options
from tola.ena.database import EnaCache
from tola.ndjson import get_input_objects
from tola.pretty import plain_text_from_itr
from tola.terminal import TerminalDict, colour_pager, s
from tola.tolqc_client import TolClient


@click.command
@click_options.tolqc_alias
@click_options.input_files
def cli(tolqc_alias, input_files):
    """
    Populate the `assembly` table with records fetched from the ENA.

    A list of accessions can be provided in ND-JSON input files under either
    `accession.id` or `accession` keys.
    """

    if input_files:
        search_accessions = search_accessions_from_files(input_files)
    else:
        # Sanger Tree of Life project accession
        search_accessions = ["PRJEB43745"]

    client = TolClient(
        tolqc_alias=tolqc_alias,
        page_size=1000,
    )
    cache = EnaCache(None, client)

    cache.cache_tolqc_assemblies()
    loaded = []
    for accession in search_accessions:
        cache.cache_ena_assemblies(accession)
        cache.load_ena_assemblies(loaded)

    cache.cache_tolqc_assemblies()
    cache.cache_tolqc_assembly_datasets()
    new_links = cache.load_ena_assembly_datasets()

    if loaded or new_links:
        itr = asm_link_dict_itr(loaded, new_links)
        if sys.stdout.isatty():
            colour_pager(itr)
        else:
            sys.stdout.write(plain_text_from_itr(itr))


def asm_link_dict_itr(loaded=None, new_links=None):
    if loaded:
        yield f"\nStored {len(loaded)} ENA assembly record{s(loaded)}:\n"
        for row in loaded:
            yield TerminalDict(row).pretty()

    if new_links:
        yield f"\nStored {len(new_links)} assembly_dataset link{s(new_links)}:\n"
        for row in new_links:
            yield TerminalDict(row).pretty()


def search_accessions_from_files(input_files) -> list[str]:
    """
    Extracts a list of accessions from ND-JSON input files.
    """
    search_acc = []
    for obj in get_input_objects(input_files):
        acc = obj.get("accession.id") or obj.get("accession")
        if acc:
            search_acc.append(acc)
    return search_acc
