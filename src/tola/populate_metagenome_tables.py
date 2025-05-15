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
@click.option(
    "--taxonomy-duckdb",
    "taxonomy_duckdb_file",
    required=True,
    help="""
        Filename of a NCBI taxonomy DuckDB database
        containing a `rankedlineage` table
        """,
)
@click_options.apply_flag
@click.pass_context
def cli(
    ctx,
    tolqc_alias,
    tolqc_url,
    api_token,
    status_duckdb_file,
    taxonomy_duckdb_file,
    apply_flag,
):
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

    client.page_size = 100

    conn = duckdb.connect(status_duckdb_file, read_only=True)
    conn.execute(f"ATTACH '{taxonomy_duckdb_file}' AS ncbi (READ_ONLY)")
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

    # Populate species by NCBI taxon_id
    species_specs = []
    for spec in iter_sql(
        conn,
        r"""
        SELECT tax_name AS 'species.id'
          , tax_id AS taxon_id
          , family AS taxon_family
          , "order" AS taxon_order
          , phylum AS taxon_phylum
          , IF(domain IS NULL
            , 'metagenomes'
            , lower(domain)) AS taxon_group
        FROM ncbi.rankedlineage
        WHERE tax_id IN (
          SELECT taxid
          FROM metagenome
          UNION
          SELECT taxid
          FROM metagenome_bin
          UNION
          SELECT m.new_tax_id
          FROM metagenome_bin AS b
          JOIN ncbi.merged AS m
            ON b.taxid = m.old_tax_id
        )
        ORDER BY ALL
        """,
    ):
        species_specs.append(spec)
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
    renamed_specimens = {
        "odPioVast1": "odPioSpec1",
    }
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

        specimen_id = row["host_tolid"]
        host_specimen_id = renamed_specimens.get(specimen_id, specimen_id)

        # Add metagenome table row
        metagenome_specs.append(
            {
                "metagenome.id": row["tolid"],
                "species.id": row["species_name"],
                "host_specimen.id": host_specimen_id,
                "biosample_accession.id": row["biosample"],
                "bioproject_accession.id": row["bioproject"],
                "assembly_accession.id": row["assembly_accession"],
                "coverage": row["coverage"],
                "version": row["version"],
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
            "metagenome_bin.id": row["tolid"],
            "metagenome.id": row["primary_tolid"],
            "species.id": row["species_name"],
            "biosample_accession.id": row["biosample"],
            "assembly_accession.id": row["assembly_accession"],
            "bin_type": bin_type_dict[row["bin_type"]],
            "ssu_count": row["SSUs"],
            "trna_total": row["total_trnas"],
            "trna_unique": row["unique_trnas"],
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
    ups.build_table_upserts(
        "specimen",
        [
            {
                # This species was present, but the specimen missing
                "specimen.id": "xgElyCris1",
                "species.id": "Elysia crispata",
            }
        ],
    )
    ups.build_table_upserts("metagenome", metagenome_specs)
    ups.build_table_upserts("metagenome_bin", metagenome_bin_specs)

    if apply_flag:
        ups.apply_upserts()
    ups.page_results(apply_flag)


def iter_table(conn, table):
    return iter_sql(
        conn,
        f"""
        SELECT IFNULL(rl.tax_name, mrl.tax_name) AS species_name
          , m.*
        FROM {table} AS m
        LEFT JOIN ncbi.rankedlineage AS rl
          ON m.taxid = rl.tax_id
        LEFT JOIN ncbi.merged
          ON m.taxid = merged.old_tax_id
        LEFT JOIN ncbi.rankedlineage AS mrl
          ON merged.new_tax_id = mrl.tax_id
        WHERE status != '9 suppressed'
        """,
    )  # noqa: S608


def iter_sql(conn, sql):
    conn.execute(sql)
    col_names = tuple(x[0] for x in conn.description)
    for row in conn.fetchall():
        yield dict(zip(col_names, row, strict=True))


if __name__ == "__main__":
    cli()
