import click
import datetime
import duckdb
import inspect
import logging
import pathlib
import sys

# from duckdb.duckdb import ConstraintException

from tola import db_connection, tolqc_client
from tola.fetch_mlwh_seq_data import write_mlwh_data_to_filehandle
from tola.ndjson import ndjson_row

from tolqc.reports import mlwh_data_report_query_select

TODAY = datetime.date.today().isoformat()  # noqa: DTZ011


def default_duckdb_file():
    return pathlib.Path(f"diff_mlwh_{TODAY}.duckdb")


def default_mlwh_ndjson_file():
    return pathlib.Path(f"mlwh_{TODAY}.ndjson")


@click.command
@tolqc_client.tolqc_alias
@tolqc_client.tolqc_url
@tolqc_client.api_token
@tolqc_client.log_level
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
    ),
    help="Name of NDJSON file from fetch-mlwh-seq-data",
    default=default_mlwh_ndjson_file(),
    show_default=True,
)
@click.option(
    "--table",
    help="Name of table for which to print patching NDJSON",
)
def cli(tolqc_alias, tolqc_url, api_token, log_level, duckdb_file, mlwh_ndjson, table):
    """Compare the contents of the MLWH to the ToLQC database"""

    logging.basicConfig(level=getattr(logging, log_level))

    have_db = duckdb_file.exists()
    con = duckdb.connect(str(duckdb_file))

    if not have_db:
        tqc = tolqc_client.TolClient(tolqc_url, api_token, tolqc_alias)
        create_diff_db(con, tqc, mlwh_ndjson)

    for tbl_name in ("mlwh", "tolqc"):
        check_for_duplicate_data_ids(con, tbl_name)
        # try:
        #     con.execute(
        #         f"CREATE UNIQUE INDEX {tbl_data_id}_data_id_udx"
        #         f"" ON {tbl_data_id} (data_id)"
        #     )
        # except ConstraintException:
        #     click.echo(f"Error: {tbl_data_id}.data_id contains duplicates", err=True)

    if table:
        col_map = table_map().get(table)
        if not col_map:
            click.exit(f"No column map for table '{table}'")
        for mismatches in compare_tables(con, "mlwh", "tolqc"):
            if len(mismatches) != 2:
                continue
            (frst_tbl, frst_name, frst), (scnd_tbl, scnd_name, scnd) = mismatches
            if not (frst_tbl == "mlwh" and scnd_tbl == "tolqc"):
                continue
            if patch := get_patch_for_table(col_map, frst, scnd):
                sys.stdout.write(ndjson_row(patch))
    else:
        for mismatches in compare_tables(con, "mlwh", "tolqc"):
            if len(mismatches) == 1:
                ((tbl_name, name, struct),) = mismatches
                click.echo(f"\nOnly in {tbl_name}: {name}")
            else:
                (frst_tbl, frst_name, frst), (scnd_tbl, scnd_name, scnd) = mismatches
                click.echo(f"\n{frst_name} {frst_tbl} | {scnd_tbl}")
                show_diff(frst, scnd)


def create_diff_db(con, tqc, mlwh_ndjson):
    # NDJSON from MLWH has different number of columns for Illumina and PacBio
    # data.  To create the same table structure for the `mlwh` and `tolqc`
    # tables we provide the column mapping.
    columns_def = column_definitions()

    # Fetch the current MLWH data from ToLQC
    tolqc_ndjson = tqc.download_file("report/mlwh-data?format=NDJSON")
    con.execute(
        f"CREATE TABLE tolqc AS FROM read_json(?, columns = {columns_def})",
        (tolqc_ndjson,),
    )

    if not mlwh_ndjson.exists():
        fetch_mlwh_seq_data_to_file(tqc, mlwh_ndjson)
    con.execute(
        f"CREATE TABLE mlwh AS FROM read_json(?, columns = {columns_def})",
        (str(mlwh_ndjson),),
    )


def fetch_mlwh_seq_data_to_file(tqc, mlwh_ndjson):
    mlwh = db_connection.mlwh_db()
    with mlwh_ndjson.open("w") as fh:
        write_mlwh_data_to_filehandle(mlwh, tqc.list_project_study_ids(), fh)


def get_patch_for_table(col_map, frst, scnd):
    diff = {}
    primary_key = None
    for key, out_key in col_map.items():
        if out_key.endswith(".id"):
            primary_key = key
            continue
        frst_v = frst[key]
        scnd_v = scnd[key]
        if frst_v != scnd_v:
            diff[out_key] = frst_v
    if not primary_key:
        click.exit(f"Failed to find primary key in: {col_map}")
    if diff:
        pk_out = col_map[primary_key]
        diff[pk_out] = frst[primary_key]
        return diff
    return None


def show_diff(frst, scnd):
    for key, frst_v in frst.items():
        scnd_v = scnd[key]
        if frst_v != scnd_v:
            click.echo(f"  {key}: {frst_v!r} | {scnd[key]!r}")


def check_for_duplicate_data_ids(con, tbl_name):
    con.execute(f"""
        SELECT data_id
        FROM {tbl_name}
        GROUP BY data_id
        HAVING COUNT(*) > 1
        ORDER BY data_id
    """)  # noqa: S608
    while row := con.fetchone():
        click.echo(f"Duplicate {tbl_name}.data_id: {row[0]}")


def compare_tables(con, frst, scnd):
    """
    Creates two tables using Common Table Expressions (CTEs, the WITH ...
    statements) from the two table name arguments `frst` and `scnd` with an
    MD5 hash of each row, joins the two tables on this `hash` column with a
    full outer join, then filters by any rows where the result from one of
    the tables is NULL. These will be the non-matching rows. Ordering by
    `data_id` enables non-matching pairs of rows to be yielded from the
    function.
    """
    sql = inspect.cleandoc(f"""
        WITH frst_h AS (
          SELECT '{frst}' AS tbl_name
            , data_id
            , md5({frst}::text) AS hash
            , {frst} AS struct
          FROM {frst}
        ),
        scnd_h AS (
          SELECT '{scnd}' AS tbl_name
            , data_id
            , md5({scnd}::text) AS hash
            , {scnd} AS struct
            FROM {scnd}
        )
        SELECT COALESCE(frst_h.tbl_name, scnd_h.tbl_name) AS tbl_name
          , COALESCE(frst_h.data_id, scnd_h.data_id) AS data_id
          , COALESCE(frst_h.struct, scnd_h.struct) AS struct
        FROM frst_h FULL JOIN scnd_h USING (hash)
        WHERE frst_h.tbl_name IS NULL
          OR scnd_h.tbl_name IS NULL
        ORDER BY data_id, tbl_name
    """)  # noqa: S608
    logging.debug(sql)
    con.execute(sql)

    prev = con.fetchone()
    while row := con.fetchone():
        if prev:
            if row[1] == prev[1]:
                # Same name
                yield prev, row
                prev = None
            else:
                yield (prev,)
                prev = row
        else:
            prev = row

    if prev:
        yield (prev,)


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


def table_map():
    query = mlwh_data_report_query_select()

    # {
    #     "name": "sample_name",
    #     "type": String(),
    #     "aliased": False,
    #     "expr": "<sqlalchemy.sql.elements.Label at 0x10bf3fe80; sample_name>",
    #     "entity": "<class 'tolqc.sample_data_models.Sample'>",
    # }

    table_map = {
        "file": {"data_id": "data.id"},
        "library": {"data_id": "data.id"},
        "pacbio_run_metrics": {"run_id": "pacbio_run_metrics.id"},
        "platform": {"run_id": "run.id"},
    }
    for desc in query.column_descriptions:
        name = desc["name"]
        tbl = desc["entity"].__tablename__
        expr = desc["expr"]
        if not hasattr(expr, "base_columns"):
            continue
        (col,) = expr.base_columns
        out_name = f"{tbl}.id" if col.primary_key else col.name
        table_map.setdefault(tbl, {})[name] = out_name

    # for tbl, mapv in table_map.items():
    #     idl = [x for x in mapv.values() if x.endswith(".id")]
    #     click.echo(f"{tbl} = {idl}", err=True)

    return table_map


def column_definitions():
    return inspect.cleandoc(
        """
        {
            data_id: 'VARCHAR',
            study_id: 'BIGINT',
            sample_name: 'VARCHAR',
            -- supplier_name: 'VARCHAR',
            tol_specimen_id: 'VARCHAR',
            biosample_accession: 'VARCHAR',
            biospecimen_accession: 'VARCHAR',
            scientific_name: 'VARCHAR',
            taxon_id: 'INTEGER',
            platform_type: 'VARCHAR',
            instrument_model: 'VARCHAR',
            instrument_name: 'VARCHAR',
            pipeline_id_lims: 'VARCHAR',
            run_id: 'VARCHAR',
            tag_index: 'VARCHAR',
            lims_run_id: 'VARCHAR',
            element: 'VARCHAR',
            run_start: 'TIMESTAMPTZ',
            run_complete: 'TIMESTAMPTZ',
            plex_count: 'INTEGER',
            lims_qc: 'VARCHAR',
            qc_date: 'TIMESTAMPTZ',
            tag1_id: 'VARCHAR',
            tag2_id: 'VARCHAR',
            library_id: 'VARCHAR',

            -- pacbio_run_metrics fields
            movie_minutes: 'INTEGER',
            binding_kit: 'VARCHAR',
            sequencing_kit: 'VARCHAR',
            sequencing_kit_lot_number: 'VARCHAR',
            cell_lot_number: 'VARCHAR',
            include_kinetics: 'VARCHAR',
            loading_conc: 'DOUBLE',
            control_num_reads: 'INTEGER',
            control_read_length_mean: 'BIGINT',
            control_concordance_mean: 'DOUBLE',
            control_concordance_mode: 'DOUBLE',
            local_base_rate: 'DOUBLE',
            polymerase_read_bases: 'BIGINT',
            polymerase_num_reads: 'INTEGER',
            polymerase_read_length_mean: 'DOUBLE',
            polymerase_read_length_n50: 'INTEGER',
            insert_length_mean: 'BIGINT',
            insert_length_n50: 'INTEGER',
            unique_molecular_bases: 'BIGINT',
            productive_zmws_num: 'INTEGER',
            p0_num: 'INTEGER',
            p1_num: 'INTEGER',
            p2_num: 'INTEGER',
            adapter_dimer_percent: 'DOUBLE',
            short_insert_percent: 'DOUBLE',
            hifi_read_bases: 'BIGINT',
            hifi_num_reads: 'INTEGER',
            hifi_read_length_mean: 'INTEGER',
            hifi_read_quality_median: 'INTEGER',
            hifi_number_passes_mean: 'DOUBLE',
            hifi_low_quality_read_bases: 'BIGINT',
            hifi_low_quality_num_reads: 'INTEGER',
            hifi_low_quality_read_length_mean: 'INTEGER',
            hifi_low_quality_read_quality_median: 'INTEGER',
            hifi_barcoded_reads: 'INTEGER',
            hifi_bases_in_barcoded_reads: 'BIGINT',

            remote_path: 'VARCHAR',
        }
    """
    )
