import datetime
import inspect
import logging
import pathlib
import sys
from functools import cache
from tempfile import NamedTemporaryFile
from typing import NamedTuple

# from duckdb.duckdb import ConstraintException
import click
import duckdb
import pyarrow
import pytz
from tolqc.reports import mlwh_data_report_query_select

from tola import db_connection, tolqc_client
from tola.fetch_mlwh_seq_data import write_mlwh_data_to_filehandle
from tola.ndjson import ndjson_row

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


@click.command
@tolqc_client.tolqc_alias
@tolqc_client.tolqc_url
@tolqc_client.api_token
@tolqc_client.log_level
@click.option(
    "--duckdb-file",
    type=click.Path(path_type=pathlib.Path),
    help="""Name of duckdb database file.
      Taken from the DIFF_MLWH_DUCKDB environment variable if set""",
    default=default_duckdb_file(),
    envvar="DIFF_MLWH_DUCKDB",
    show_default=True,
)
@click.option(
    "--mlwh-ndjson",
    type=click.Path(
        path_type=pathlib.Path,
    ),
    envvar="MLWH_NDJSON",
    help="""Name of NDJSON file from fetch-mlwh-seq-data.
      Taken from the MLWH_NDJSON environment variable if set""",
)
@click.option(
    "--update/--no-update",
    help="Update ToLQC and MLWH data from their source databases",
    default=False,
    show_default=True,
)
@click.option(
    "--new/--quiet",
    "show_new_diffs",
    help="Print the most recently detected differences to STDOUT",
    default=True,
    show_default=True,
)
@click.option(
    "--show-classes",
    flag_value=True,
    help="""Show the list of differences grouped by the names of columns
      which differ and their counts""",
)
@click.option(
    "--format",
    type=click.Choice(
        ["PRETTY", "NDJSON"],
        case_sensitive=False,
    ),
    default="PRETTY",
    show_default=True,
    help="""Output differences found in either 'PRETTY'
      (human readable) or 'NDJSON' format""",
)
@click.option(
    "--today",
    "since",
    flag_value=TODAY,
    type=click.DateTime(),
    help="Only show differences found today",
)
@click.option(
    "--since",
    "since",
    type=click.DateTime(),
    help="Show differences detected since a particular date or time",
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
    show_new_diffs,
    show_classes,
    format,
    since,
    table,
    update,
):
    """Compare the contents of the MLWH to the ToLQC database"""

    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(message)s",
        force=True,
    )

    conn = duckdb.connect(str(duckdb_file))

    tqc = tolqc_client.TolClient(tolqc_url, api_token, tolqc_alias)
    create_diff_db(conn, tqc, mlwh_ndjson, update)

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
    elif show_classes:
        show_diff_classes(conn)
    elif since or show_new_diffs:
        show_stored_diffs(conn, since, show_new_diffs)


def create_diff_db(conn, tqc, mlwh_ndjson=None, update=False):
    create_diff_store(conn)

    tables = {x[0] for x in conn.execute("SHOW TABLES").fetchall()}

    # Fetch the current MLWH data from ToLQC
    if update or "tolqc" not in tables:
        tolqc_tmp = NamedTemporaryFile("r", prefix="tolqc_", suffix=".ndjson")
        logging.info(
            f"Downloading current data from {tqc.tolqc_alias} into {tolqc_tmp.name}"
        )
        tqc.download_file("report/mlwh-data?format=NDJSON", tolqc_tmp.name)
        load_table_from_json(conn, "tolqc", tolqc_tmp.name)

    if update or "mlwh" not in tables:
        if mlwh_ndjson and mlwh_ndjson.exists():
            load_table_from_json(conn, "mlwh", str(mlwh_ndjson))
        else:
            mlwh_tmp = NamedTemporaryFile(
                "w", prefix="mlwh_", suffix=".ndjson", delete_on_close=False
            )
            logging.info(f"Downloading data from MLWH into {mlwh_tmp.name}")
            fetch_mlwh_seq_data_to_file(tqc, mlwh_tmp)
            load_table_from_json(conn, "mlwh", mlwh_tmp.name)


def load_table_from_json(conn, name, file):
    logging.info(f"Loading {file} into {name} table")

    # NDJSON from MLWH has different number of columns for Illumina and PacBio
    # data.  To create the same table structure for the `mlwh` and `tolqc`
    # tables we provide the column mapping.
    table_cols, json_cols = column_definitions()

    conn.execute(f"CREATE OR REPLACE TABLE {name}({table_cols})")
    conn.execute(
        f"INSERT INTO {name} FROM read_json(?, columns = {{{json_cols}}})", (file,)
    )


def fetch_mlwh_seq_data_to_file(tqc, mlwh_ndjson):
    mlwh = db_connection.mlwh_db()
    write_mlwh_data_to_filehandle(mlwh, tqc.list_project_study_ids(), mlwh_ndjson)
    mlwh_ndjson.close()


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


def show_stored_diffs(conn, since=None, show_new_diffs=False):
    sql = inspect.cleandoc("""
        SELECT ds.data_id
          , ds.differing_columns
          , mlwh
          , tolqc
        FROM diff_store ds
        JOIN mlwh USING (data_id)
        JOIN tolqc USING (data_id)
    """)
    if since:
        sql += "\nWHERE ds.found_at >= ?"
        debug_sql(sql)
        conn.execute(sql, (since,))
    elif show_new_diffs:
        sql += "\nWHERE ds.found_at = (SELECT MAX(found_at) FROM diff_store)"
        debug_sql(sql)
        conn.execute(sql)
    else:
        debug_sql(sql)
        conn.execute(sql)

    while diff := conn.fetchone():
        data_id, cols, mlwh, tolqc = diff
        click.echo(f"\n{data_id = }\n{cols = }")


def show_diff_classes(conn):
    sql = inspect.cleandoc("""
        SELECT count(*) AS n
          , differing_columns
        FROM diff_store
        GROUP BY differing_columns
        ORDER BY differing_columns
    """)
    debug_sql(sql)
    conn.execute(sql)
    while c := conn.fetchone():
        n, cols = c
        click.echo(f"{n:>7}  {','.join(cols)}")


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

    conn.execute("SET temp_directory = '/tmp'")
    for name in ("mlwh", "tolqc"):
        build_h_table(conn, name)
        cleanup_diff_store(conn, name)

    sql = inspect.cleandoc("""
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
    debug_sql(sql)
    conn.execute(sql)

    while row := conn.fetchone():
        yield Mismatch._make(row)


def debug_sql(sql):
    logging.debug(f"\n{sql};")


def build_h_table(conn, name):
    """
    Create tempoary table with a hash of each row in table `name`
    """
    sql = inspect.cleandoc(f"""
        CREATE TEMPORARY TABLE {name}_h AS
          SELECT data_id
            , md5({name}::VARCHAR) AS hash
            , {name} AS struct
          FROM {name}
    """)  # noqa: S608
    debug_sql(sql)
    conn.execute(sql)


def cleanup_diff_store(conn, name):
    """
    Remove any rows in `diff_store` which no longer match the current row in
    the hash table
    """
    sql = inspect.cleandoc(f"""
        DELETE FROM diff_store
        WHERE data_id IN (
          SELECT data_id
          FROM diff_store
          ANTI JOIN {name}_h AS h
            ON diff_store.{name}_hash = h.hash
        )
    """)  # noqa: S608
    debug_sql(sql)
    conn.execute(sql)


def create_diff_store(conn):
    sql = inspect.cleandoc(
        """
        CREATE TABLE IF NOT EXISTS diff_store(
            data_id VARCHAR PRIMARY KEY
            , mlwh_hash VARCHAR
            , tolqc_hash VARCHAR
            , differing_columns VARCHAR[]
            , found_at TIMESTAMP WITH TIME ZONE
        )
        """
    )
    debug_sql(sql)
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
    for name, tbl, col in name_table_column(query):
        out_name = f"{tbl}.id" if col.primary_key else col.name
        table_map.setdefault(tbl, {})[name] = out_name

    # for tbl, mapv in table_map.items():
    #     idl = [x for x in mapv.values() if x.endswith(".id")]
    #     click.echo(f"{tbl} = {idl}", err=True)

    return table_map


def name_table_column(query):
    """
    Returns a list of tuples (name, Table, Column) for each column of the
    SQLAlchemy `query` argument.
    """
    cols = []
    for desc in query.column_descriptions:
        name = desc["name"]
        tbl = desc["entity"].__tablename__
        expr = desc["expr"]
        if hasattr(expr, "base_columns"):
            (col,) = expr.base_columns
        else:
            (col,) = expr.columns.values()
        cols.append((name, tbl, col))

    return cols


@cache
def column_definitions():
    query = mlwh_data_report_query_select()
    col_defs = {}

    debug_str = "Column types from mlwh-data query:\n"
    for name, _, col in name_table_column(query):
        if name == "supplier_name":
            continue
        type_ = "TIMESTAMPTZ" if (s := str(col.type)) == "DATETIME" else s
        debug_str += f"  {name} = {type_}\n"
        col_defs[name] = type_
    logging.debug(debug_str)

    table_cols = "\n, ".join(
        f"{n} {t} PRIMARY KEY" if n == "data_id" else f"{n} {t}"
        for n, t in col_defs.items()
    )

    json_cols = "\n, ".join(f"{n}: '{t}'" for n, t in col_defs.items())

    return (table_cols, json_cols)
