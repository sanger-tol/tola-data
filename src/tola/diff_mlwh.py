import click
import datetime
import duckdb
import inspect
import logging
import pathlib
import pyarrow
import pytz
import sys

from typing import NamedTuple

# from duckdb.duckdb import ConstraintException

from tola import db_connection, tolqc_client
from tola.fetch_mlwh_seq_data import write_mlwh_data_to_filehandle
from tola.ndjson import ndjson_row

from tolqc.reports import mlwh_data_report_query_select

TODAY = datetime.date.today().isoformat()  # noqa: DTZ011


class Mismatch(NamedTuple):
    """Stores a row from the diff query"""

    data_id: str
    mlwh: str
    tolqc: str
    mlwh_hash: str
    tolqc_hash: str


class DiffStore:
    """Accumulates diff results"""

    def __init__(self):
        self.data_id = []
        self.mlwh_hash = []
        self.tolqc_hash = []
        self.differing_columns = []

    def add(self, m: Mismatch):
        self.data_id.append(m.data_id)
        self.mlwh_hash.append(m.mlwh_hash)
        self.tolqc_hash.append(m.tolqc_hash)

        mlwh = m.mlwh
        tolqc = m.tolqc
        diff_cols = []
        for fld in mlwh:
            if mlwh[fld] != tolqc[fld]:
                diff_cols.append(fld)
        if not diff_cols:
            msg = f"Failed to find any differing columns in:\n{mlwh = }\n{tolqc = }"
            raise ValueError(msg)

        self.differing_columns.append(diff_cols)

    def arrow_table(self, now):
        return pyarrow.Table.from_pydict(
            {
                "data_id": pyarrow.array(self.data_id),
                "mlwh_hash": pyarrow.array(self.mlwh_hash),
                "tolqc_hash": pyarrow.array(self.tolqc_hash),
                "differing_columns": pyarrow.array(self.differing_columns),
                "found_at": pyarrow.array([now] * len(self.data_id)),
            }
        )

    def store(self, conn):
        count = len(self.data_id)
        if count == 0:
            logging.info("No new differences found")
            return
        logging.info(f"Found {count} new differences")
        (tz_name,) = conn.execute("SELECT current_setting('TimeZone')").fetchone()
        now = datetime.datetime.now(tz=pytz.timezone(tz_name))
        arrow_table__ = self.arrow_table(now)  # noqa: F841
        conn.sql("INSERT INTO diff_store SELECT * FROM arrow_table__")


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
    "--update/--no-update",
    help="Update ToLQC and MLWH data from their source databases",
    default=False,
    show_default=True,
)
@click.option(
    "--table",
    help="Name of table for which to print patching NDJSON",
)
def cli(
    tolqc_alias,
    tolqc_url,
    api_token,
    log_level,
    duckdb_file,
    mlwh_ndjson,
    table,
    update,
):
    """Compare the contents of the MLWH to the ToLQC database"""

    logging.basicConfig(level=getattr(logging, log_level))

    conn = duckdb.connect(str(duckdb_file))

    tqc = tolqc_client.TolClient(tolqc_url, api_token, tolqc_alias)
    create_diff_db(conn, tqc, mlwh_ndjson, update)

    for tbl_name in ("mlwh", "tolqc"):
        check_for_duplicate_data_ids(conn, tbl_name)
        # try:
        #     conn.execute(
        #         f"CREATE UNIQUE INDEX {tbl_data_id}_data_id_udx"
        #         f"" ON {tbl_data_id} (data_id)"
        #     )
        # except ConstraintException:
        #     click.echo(f"Error: {tbl_data_id}.data_id contains duplicates", err=True)

    ds = DiffStore()
    for m in compare_tables(conn):
        ds.add(m)
    ds.store(conn)

    if table:
        col_map = table_map().get(table)
        if not col_map:
            exit(f"No column map for table '{table}'")
        for m in compare_tables(conn):
            if patch := get_patch_for_table(col_map, m.mlwh, m.tolqc):
                sys.stdout.write(ndjson_row(patch))


def create_diff_db(conn, tqc, mlwh_ndjson, update=False):
    # NDJSON from MLWH has different number of columns for Illumina and PacBio
    # data.  To create the same table structure for the `mlwh` and `tolqc`
    # tables we provide the column mapping.
    columns_def = column_definitions()
    create_diff_store(conn)

    tables = {x[0] for x in conn.execute("SHOW TABLES").fetchall()}

    # Fetch the current MLWH data from ToLQC
    if update or "tolqc" not in tables:
        logging.info(f"Downloading current data from {tqc.tolqc_alias}")
        tolqc_ndjson = tqc.download_file("report/mlwh-data?format=NDJSON")
        logging.info("Loading {tolqc_ndjson} into tolqc table")
        conn.execute(
            (
                "CREATE OR REPLACE TABLE tolqc"
                f" AS FROM read_json(?, columns = {columns_def})"
            ),
            (tolqc_ndjson,),
        )

    if update or "mlwh" not in tables:
        if not mlwh_ndjson.exists():
            logging.info(f"Downloading data from MLWH into {mlwh_ndjson}")
            fetch_mlwh_seq_data_to_file(tqc, mlwh_ndjson)
        logging.info(f"Loading {mlwh_ndjson} into mlwh table")
        conn.execute(
            (
                "CREATE OR REPLACE TABLE mlwh"
                f" AS FROM read_json(?, columns = {columns_def})"
            ),
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
        exit(f"Failed to find primary key in: {col_map}")
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


def check_for_duplicate_data_ids(conn, tbl_name):
    conn.execute(f"""
        SELECT data_id
        FROM {tbl_name}
        GROUP BY data_id
        HAVING COUNT(*) > 1
        ORDER BY data_id
    """)  # noqa: S608
    while row := conn.fetchone():
        click.echo(f"Duplicate {tbl_name}.data_id: {row[0]}")


def compare_tables(conn):
    """
    Creates two tables using Common Table Expressions (CTEs, the WITH ...
    statements) from the two tables `mlwh` and `tolqc`. The tables contain
    the `data_id`, an MD5 hash of the entire row cast to VARCHAR, and the row
    itself as a DuckDB STRUCT.

    The join returns any rows where the `data_id` matches but the MD5 hash
    does not.

    ANTI JOINs to the `diff_store` table ignore any mismatches which have
    already been seen.
    """

    sql = inspect.cleandoc("""
        WITH mlwh_h AS (
          SELECT data_id
            , md5(mlwh::VARCHAR) AS hash
            , mlwh AS struct
          FROM mlwh
        ),
        tolqc_h AS (
          SELECT data_id
            , md5(tolqc::VARCHAR) AS hash
            , tolqc AS struct
          FROM tolqc
        )
        SELECT mlwh_h.data_id
          , mlwh_h.struct AS mlwh_struct
          , tolqc_h.struct AS tolqc_struct
          , mlwh_h.hash AS mlwh_hash
          , tolqc_h.hash AS tolqc_hash
        FROM mlwh_h JOIN tolqc_h
          ON mlwh_h.data_id = tolqc_h.data_id
          AND mlwh_h.hash != tolqc_h.hash
        ANTI JOIN diff_store AS mds
          ON mlwh_h.hash = mds.mlwh_hash
        ANTI JOIN diff_store AS qds
          ON tolqc_h.hash = qds.tolqc_hash
        ORDER BY mlwh_h.data_id
    """)
    logging.debug(sql)
    conn.execute(sql)

    while row := conn.fetchone():
        yield Mismatch._make(row)


def copy_data_to_dest(conn, source, dest):
    get_cols = inspect.cleandoc("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'main'
          AND table_name = ?
        ORDER BY ordinal_position
    """)
    conn.execute(get_cols, (dest,))
    col_list = "\n  , ".join(x[0] for x in conn.fetchall())
    conn.execute(f"INSERT INTO {dest}\nSELECT {col_list}\nFROM {source}")

def create_diff_store(conn):
    sql = inspect.cleandoc(
        """
        CREATE TABLE IF NOT EXISTS diff_store(
            data_id VARCHAR
            , mlwh_hash VARCHAR
            , tolqc_hash VARCHAR
            , differing_columns VARCHAR[]
            , found_at TIMESTAMP WITH TIME ZONE
        )
        """
    )
    logging.debug(sql)
    conn.execute(sql)

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
        "pacbio_run_metrics": {"run_id": "pacbio_run_metrics.id"},
        "platform": {"run_id": "run.id"},
    }
    for desc in query.column_descriptions:
        name = desc["name"]
        tbl = desc["entity"].__tablename__
        expr = desc["expr"]
        if hasattr(expr, "base_columns"):
            (col,) = expr.base_columns
        else:
            (col,) = expr.columns.values()
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
