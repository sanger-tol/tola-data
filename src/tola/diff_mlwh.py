import click
import datetime
import duckdb
import inspect
import pathlib

from tola import tolqc_client

TODAY = datetime.date.today().isoformat()  # noqa: DTZ011


def default_duckdb_file():
    return pathlib.Path(f"diff_mlwh_{TODAY}.duckdb")


def default_mlwh_ndjson_file():
    return pathlib.Path(f"mlwh_{TODAY}.ndjson")


@click.command
@tolqc_client.tolqc_alias
@tolqc_client.tolqc_url
@tolqc_client.api_token
@click.option(
    "--duckdb-file",
    type=click.Path(path_type=pathlib.Path),
    help="Name of duckdb database file.",
    default=default_duckdb_file(),
    show_default=True,
)
@click.option(
    "--mlwh-ndjson",
    type=click.Path(
        path_type=pathlib.Path,
        exists=True,
        readable=True,
    ),
    help="Name of NDJSON file from fetch-mlwh-seq-data",
    default=default_mlwh_ndjson_file(),
    show_default=True,
)
def cli(tolqc_alias, tolqc_url, api_token, duckdb_file, mlwh_ndjson):
    """Compare the contents of the MLWH to the ToLQC database"""

    tqc = tolqc_client.TolClient(tolqc_url, api_token, tolqc_alias)
    con = duckdb.connect(str(duckdb_file))
    con.execute(diff_db_table_definition("mlwh"))
    con.execute(diff_db_table_definition("tolqc"))

    # NDJSON from MLWH has different number of columns for Illumina and PacBio
    # data, and the created table `temp` may have the columns in a different
    # order to our schema, so we need to load then copy the data into the
    # `mlwh` table.
    con.execute("CREATE TABLE temp AS FROM read_json(?)", (str(mlwh_ndjson),))
    copy_data_to_dest(con, "temp", "mlwh")
    con.execute("DROP TABLE temp")

    # Fetch the current MLWH data from ToLQC
    con.execute("SET force_download=true")
    con.execute(
        "INSERT INTO tolqc FROM read_json(?)",
        (tqc.build_path("report/mlwh-data?format=NDJSON"),),
    )


def copy_data_to_dest(con, source, dest):
    get_cols = inspect.cleandoc("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'main'
          AND table_name = ?
        ORDER BY ordinal_position
    """)
    con.execute(get_cols, (dest,))
    col_list = "\n  , ".join(x[0] for x in con.fetchall())
    con.execute(f"INSERT INTO {dest}\nSELECT {col_list}\nFROM {source}")


def diff_db_table_definition(table_name):
    return inspect.cleandoc(
        """
        CREATE TABLE {}(
            "name" VARCHAR
          , study_id BIGINT
          , sample_name VARCHAR
          , supplier_name VARCHAR
          , tol_specimen_id VARCHAR
          , biosample_accession VARCHAR
          , biospecimen_accession VARCHAR
          , scientific_name VARCHAR
          , taxon_id INTEGER
          , platform_type VARCHAR
          , instrument_model VARCHAR
          , instrument_name VARCHAR
          , pipeline_id_lims VARCHAR
          , run_id VARCHAR
          , tag_index VARCHAR
          , lims_run_id VARCHAR
          , element VARCHAR
          , run_start VARCHAR
          , run_complete VARCHAR
          , plex_count BIGINT
          , lims_qc VARCHAR
          , qc_date VARCHAR
          , tag1_id VARCHAR
          , tag2_id VARCHAR
          , library_id VARCHAR

          -- pacbio_run_metrics columns
          , movie_minutes INTEGER
          , binding_kit VARCHAR
          , sequencing_kit VARCHAR
          , sequencing_kit_lot_number VARCHAR
          , cell_lot_number VARCHAR
          , include_kinetics VARCHAR
          , loading_conc DOUBLE
          , control_num_reads INTEGER
          , control_read_length_mean BIGINT
          , control_concordance_mean DOUBLE
          , control_concordance_mode DOUBLE
          , local_base_rate DOUBLE
          , polymerase_read_bases BIGINT
          , polymerase_num_reads INTEGER
          , polymerase_read_length_mean DOUBLE
          , polymerase_read_length_n50 INTEGER
          , insert_length_mean BIGINT
          , insert_length_n50 INTEGER
          , unique_molecular_bases BIGINT
          , productive_zmws_num INTEGER
          , p0_num INTEGER
          , p1_num INTEGER
          , p2_num INTEGER
          , adapter_dimer_percent DOUBLE
          , short_insert_percent DOUBLE
          , hifi_read_bases BIGINT
          , hifi_num_reads INTEGER
          , hifi_read_length_mean INTEGER
          , hifi_read_quality_median INTEGER
          , hifi_number_passes_mean DOUBLE
          , hifi_low_quality_read_bases BIGINT
          , hifi_low_quality_num_reads INTEGER
          , hifi_low_quality_read_length_mean INTEGER
          , hifi_low_quality_read_quality_median INTEGER
          , hifi_barcoded_reads INTEGER
          , hifi_bases_in_barcoded_reads BIGINT

          , remote_path VARCHAR
        )
    """
    ).format(table_name)
