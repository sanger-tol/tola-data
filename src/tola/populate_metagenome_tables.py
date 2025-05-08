import sys

import click
import duckdb

from tola import click_options, tolqc_client
from tola.db_connection import ConnectionParamsError
from tola.pretty import bold, setup_pager
from tola.tqc.upsert import TableUpserter


@click.command
@click_options.tolqc_alias
@click_options.tolqc_url
@click_options.api_token
@click.option(
    "--duckdb",
    "status_duckdb_file",
    required=True,
    help="Filename of the DuckDB database created by `status-duckdb`",
)
@click_options.apply_flag
@click.pass_context
def cli(ctx, tolqc_alias, tolqc_url, api_token, status_duckdb_file, apply_flag):
    """
    Populate the metagenome tables from a DuckDB database of the status Google
    sheet created by the `status-duckdb` script.
    """
    setup_pager()
    try:
        client = tolqc_client.TolClient(tolqc_url, api_token, tolqc_alias)
    except ConnectionParamsError as cpe:
        if sys.stdout.isatty():
            # Show help if we're on a TTY
            err = "Error: " + bold("\n".join(cpe.args))
            sys.exit(ctx.get_help() + "\n\n" + err)
        else:
            sys.exit("\n".join(cpe.args))

    conn = duckdb.connect(status_duckdb_file, read_only=True)
    ups = TableUpserter(client)
    ups.build_table_upserts(
        "bin_type_dict",
        [
            {
                "bin_type_dict.id": "BIN",
                "description": "binned contigs",
            },
            {
                "bin_type_dict.id": "MAG",
                "description": "assembled metagenome",
            },
        ],
    )

    species_specs = []
    for species_id, taxon_id in conn.execute(
        """
        SELECT MAX(species_id)
          , taxon_id
        FROM (
          SELECT taxname AS species_id
            , taxid AS taxon_id
          FROM metagenome
          UNION
          SELECT taxname AS species_id
            , taxid AS taxon_id
          FROM metagenome_bin
        )
        GROUP BY taxon_id
        """
    ).fetchall():
        species_specs.append(
            {
                "species.id": species_id,
                "taxon_id": taxon_id,
            }
        )
    ups.build_table_upserts("species", species_specs)

    #          host_tolid = odSpoBarb1
    #        host_taxname = Spongia barbara
    #          host_taxid = 1979531
    #      host_biosample = SAMEA115462094
    #             project = asg
    #               tolid = odSpoBarb1.metagenome
    #             taxname = sponge metagenome
    #               taxid = 1163772
    #                mags = 5
    #  binned_metagenomes = 2
    #           biosample = SAMEA118088412
    #          bioproject = PRJEB88818
    #  assembly_accession = ERZ26695794
    #              status = 1 submitted
    #             comment = done
    #            coverage = 31
    #             version = NULL
    #            column17 = NULL
    #      run_accessions = [ERR14842679, ERR14842680, ERR14857143]
    accession_specs = []
    metagenome_specs = []
    col_acc_types = {
        "biosample": "BioSample",
        "bioproject": "BioProject",
        "assembly_accession": "Analysis",
    }
    for row in iter_table(conn, "metagenome"):
        # Add accessions
        for col_name, acc_type in col_acc_types.items():
            if acc := row.get(col_name):
                accession_specs.append(
                    {
                        "accession.id": acc,
                        "accession_type.id": acc_type,
                    }
                )

        # Add metagenome table row
        metagenome_specs.append(
            {
                "metagenome.id": row.get("tolid"),
                "species.id": row.get("taxname"),
                "biosample_accession.id": row.get("biosample"),
                "bioproject_accession.id": row.get("bioproject"),
                "assembly_accession.id": row.get("assembly_accession"),
                "coverage": row.get("coverage"),
                "version": row.get("version"),
            }
        )

    #                host = odDiaEryt1
    #       primary_tolid = odDiaEryt1.metagenome
    #   primary_biosample = SAMEA115283595
    #               tolid = odDiaEryt1.Gammaproteobacteria_5
    #             taxname = Gammaproteobacteria bacterium
    #               taxid = 1913989
    #            bin_type = binned metagenome
    #              length = 2609406
    #             contigs = 24
    #    circular_contigs = 0
    #        completeness = 78.76
    #       contamination = 2.05
    #       mean_coverage = 9.82031
    #                SSUs = 1
    #         total_trnas = 44
    #        unique_trnas = 21
    #             has_23s = true
    #             has_16s = true
    #              has_5s = true
    #           biosample = SAMEA115283611
    #          bioproject = PRJEB72581
    #  assembly_accession = ERZ25068038
    #              status = 1 submitted
    #             comment = NULL
    #            column24 = NULL
    #      old_bioproject = NULL
    col_acc_types.pop("bioproject")
    metagenome_bin_specs = []
    bin_type_dict = {
        "binned metagenome": "BIN",
        "MAG": "MAG",
    }
    for row in iter_table(conn, "metagenome_bin"):
        # Add accessions
        for col_name, acc_type in col_acc_types.items():
            if acc := row.get(col_name):
                accession_specs.append(
                    {
                        "accession.id": acc,
                        "accession_type.id": acc_type,
                    }
                )

        # Add metagenome table row
        spec = {
            "metagenome_bin.id": row.get("tolid"),
            "metagenome.id": row.get("primary_tolid"),
            "species.id": row.get("taxname"),
            "biosample_accession.id": row.get("biosample"),
            "assembly_accession.id": row.get("assembly_accession"),
            "bin_type": bin_type_dict.get(row.get("bin_type")),
            "ssu_count": row.get("SSUs"),
            "trna_total": row.get("total_trnas"),
            "trna_unique": row.get("unique_trnas"),
        }
        for col_name in (
            "length",
            "contigs",
            "circular_contigs",
            "completeness",
            "contamination",
            "mean_coverage",
            "has_23s",
            "has_16s",
            "has_5s",
        ):
            spec[col_name] = row.get(col_name)

        metagenome_bin_specs.append(spec)

    ups.build_table_upserts("accession", accession_specs)
    ups.build_table_upserts("metagenome", metagenome_specs)
    ups.build_table_upserts("metagenome_bin", metagenome_bin_specs)

    if apply_flag:
        ups.apply_upserts()
    ups.page_results(apply_flag)


def iter_table(conn, table):
    conn.execute(f"FROM {table}")
    col_names = tuple(x[0] for x in conn.description)
    for row in conn.fetchall():
        yield {c: x for c, x in zip(col_names, row, strict=True)}  # noqa: C416


if __name__ == "__main__":
    cli()
