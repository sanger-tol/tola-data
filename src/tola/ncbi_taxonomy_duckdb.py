import datetime
from inspect import cleandoc
from pathlib import Path

import click
import duckdb


@click.command
@click.option(
    "--taxonomy-dir",
    help="Directory containing NCBI taxonony `*.dmp` files",
    type=click.Path(
        exists=True,
        file_okay=False,
        path_type=Path,
    ),
    required=True,
)
def cli(taxonomy_dir):
    """
    Create a DuckDB database from the NCBI Taxonomy archive:

      https://ftp.ncbi.nlm.nih.gov/pub/taxonomy/new_taxdump/new_taxdump.tar.gz

    Column names are taken from:

      https://ftp.ncbi.nlm.nih.gov/pub/taxonomy/new_taxdump/taxdump_readme.txt

    lowercased and with spaces replaced by underscores.

    Tables are given the same name as the `.dmp` file, except that they are
    de-pluralised by removing trailing `s` characters.

    Where the last column in a table is a list as a punctuated string it is
    coverted to a DuckDB list.
    """

    table_config = {
        "nodes": {
            "tax_id": "INTEGER",
            "parent_tax_id": "INTEGER",
            "rank": "VARCHAR",
            "embl_code": "VARCHAR",
            "division_id": "INTEGER",
            "inherited_div": "BOOLEAN",
            "genetic_code_id": "INTEGER",
            "inherited_gc": "BOOLEAN",
            "mitochondrial_genetic_code_id": "INTEGER",
            "inherited_mgc": "BOOLEAN",
            "genbank_hidden": "BOOLEAN",
            "hidden_subtree_root": "BOOLEAN",
            "comments": "VARCHAR",
            "plastid_genetic_code_id": "VARCHAR",
            "inherited_pgc": "BOOLEAN",
            "specified_species": "VARCHAR",
            "hydrogenosome_genetic_code_id": "INTEGER",
            "inherited_hgc": "BOOLEAN",
        },
        "names": {
            "tax_id": "INTEGER",
            "name_txt": "VARCHAR",
            "unique_name": "VARCHAR",
            "name_class": "VARCHAR",
        },
        "division": {
            "division_id": "INTEGER",
            "division_cde": "VARCHAR",
            "division_name": "VARCHAR",
            "comments": "VARCHAR",
        },
        "gencode": {
            "genetic_code_id": "VARCHAR",
            "abbreviation": "VARCHAR",
            "name": "VARCHAR",
            "cde": "VARCHAR",
            "starts": "VARCHAR",
        },
        "delnodes": {
            "tax_id": "INTEGER",
        },
        "merged": {
            "old_tax_id": "INTEGER",
            "new_tax_id": "INTEGER",
        },
        "citations": {
            "cit_id": "INTEGER",
            "cit_key": "VARCHAR",
            "medline_id": "INTEGER",
            "pubmed_id": "INTEGER",
            "url": "VARCHAR",
            "text": "VARCHAR",
            "taxid_list": ("INTEGER[]", r" "),
        },
        "typeoftype": {
            "type_name": "VARCHAR",
            "synonyms": "VARCHAR",
            "nomenclature": "VARCHAR",
            "description": "VARCHAR",
        },
        "host": {
            "tax_id": "INTEGER",
            "potential_hosts": ("VARCHAR[]", r","),
        },
        "typematerial": {
            "tax_id": "INTEGER",
            "tax_name": "VARCHAR",
            "type": "VARCHAR",
            "identifier": "VARCHAR",
        },
        "rankedlineage": {
            "tax_id": "INTEGER",
            "tax_name": "VARCHAR",
            "species": "VARCHAR",
            "genus": "VARCHAR",
            "family": "VARCHAR",
            "order": "VARCHAR",
            "class": "VARCHAR",
            "phylum": "VARCHAR",
            "kingdom": "VARCHAR",
            "realm": "VARCHAR",
            "domain": "VARCHAR",
        },
        "fullnamelineage": {
            "tax_id": "INTEGER",
            "tax_name": "VARCHAR",
            "lineage": ("VARCHAR[]", r"; "),
        },
        "taxidlineage": {
            "tax_id": "INTEGER",
            "lineage": ("INTEGER[]", r" "),
        },
        "excludedfromtype": {
            "tax_id": "INTEGER",
            "tax_name": "VARCHAR",
            "property": "VARCHAR",
            "voucher_strain": "VARCHAR",
        },
        "images": {
            "image_id": "INTEGER",
            "image_key": "VARCHAR",
            "url": "VARCHAR",
            "license": "VARCHAR",
            "attribution": "VARCHAR",
            "source": "VARCHAR",
            "properties": "VARCHAR",
            "taxid_list": ("INTEGER[]", r" "),
        },
    }

    duckdb_file = f"ncbi_taxonomy_{datetime.date.today().isoformat()}.duckdb"  # noqa: DTZ011
    click.echo(f"Creating DuckDB database: {duckdb_file!r}", err=True)
    conn = duckdb.connect(duckdb_file)

    for file_root, config in table_config.items():
        file = taxonomy_dir / f"{file_root}.dmp"

        # Table names should not be plural
        table = file_root.rstrip("s")

        if file.exists():
            build_taxonomy_table(conn, table, file, config)
        else:
            click.echo(f"No such file: {file!r}", err=True)

    # Cleanup citation table
    conn.execute("UPDATE citation SET medline_id = NULL WHERE medline_id = 0")
    conn.execute("UPDATE citation SET pubmed_id = NULL WHERE pubmed_id = 0")


def build_taxonomy_table(conn, table, file, config):
    col_names = list(config)
    last_col = col_names[-1]
    last_col_type = config[last_col]
    parse_config = config.copy()
    parse_config[last_col] = "VARCHAR"

    # Need to trim the trailing two '\t|' characters from the last field
    last_trim = f"{last_col}[:-3]"
    if isinstance(last_col_type, tuple):
        array_type, sep = last_col_type
        cast_sql = (
            rf"[], CAST(string_split(rtrim({last_trim}, '{sep}'), '{sep}')"
            f" AS {array_type})"
        )
    else:
        cast_sql = f"NULL, CAST({last_trim} AS {last_col_type})"

    sql = cleandoc(rf"""
        CREATE TABLE {table}
        AS SELECT * EXCLUDE ({last_col})
          , IF(length({last_col}) = 2, {cast_sql}) AS {last_col}
        FROM read_csv(?, sep='\t|\t', columns={parse_config!r})
        """)  # noqa: S608
    click.echo(f"\n{sql};", err=True)
    conn.execute(sql, [str(file)])


if __name__ == "__main__":
    cli()
