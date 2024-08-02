import click

from tola import click_options, tolqc_client
from tola.fetch_mlwh_seq_data import patch_species


@click.command
@click_options.tolqc_alias
@click_options.tolqc_url
@click_options.api_token
def cli(tolqc_alias, tolqc_url, api_token):
    """Fill in species which have empty taxon... columns"""
    client = tolqc_client.TolClient(tolqc_url, api_token, tolqc_alias)
    patch_species(client)


if __name__ == "__main__":
    cli()
