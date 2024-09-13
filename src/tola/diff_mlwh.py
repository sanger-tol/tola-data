import inspect
import io
import logging
import pathlib
import sys
from functools import cache
from tempfile import NamedTemporaryFile
from typing import Any

import click
import duckdb
import pyarrow
from tolqc.reports import mlwh_data_report_query_select

from tola import click_options, db_connection, fetch_mlwh_seq_data, tolqc_client
from tola.ndjson import ndjson_row
from tola.pretty import bold, field_style


class Mismatch:
    """Stores a row from the diff query"""

    __slots__ = (
        "data_id",
        "mlwh",
        "tolqc",
        "mlwh_hash",
        "tolqc_hash",
        "differing_columns",
    )

    def __init__(
        self,
        data_id: str,
        mlwh: dict[str, Any],
        tolqc: dict[str, Any],
        mlwh_hash: str,
        tolqc_hash: str,
        differing_columns: list[str] = None,
    ):
        self.data_id = data_id
        self.mlwh = mlwh
        self.tolqc = tolqc
        self.mlwh_hash = mlwh_hash
        self.tolqc_hash = tolqc_hash
        if differing_columns:
            self.differing_columns = differing_columns
        else:
            self._build_differing_columns()

    @property
    def diff_class(self) -> list[str]:
        return ",".join(self.differing_columns)

    @property
    def differences_dict(self):
        dd = {"data_id": self.data_id}
        for col in self.differing_columns:
            dd[col] = (self.mlwh[col], self.tolqc[col])
        return dd

    def _build_differing_columns(self):
        mlwh = self.mlwh
        tolqc = self.tolqc

        diff_cols = []
        for fld in mlwh:
            if mlwh[fld] != tolqc[fld]:
                diff_cols.append(fld)
        if not diff_cols:
            msg = f"Failed to find any differing columns in:\n{mlwh = }\n{tolqc = }"
            raise ValueError(msg)

        self.differing_columns = diff_cols

    def get_patch_for_table(self, table, col_map):
        mlwh = self.mlwh
        tolqc = self.tolqc

        patch = {}
        primary_key = None
        for key, out_key in col_map.items():
            if out_key == table + ".id":
                primary_key = key
                continue
            mlwh_v = mlwh[key]
            tolqc_v = tolqc[key]
            if mlwh_v != tolqc_v:
                patch[out_key] = mlwh_v
        if not primary_key:
            msg = f"Failed to find primary key in: {col_map}"
            raise ValueError(msg)
        if patch:
            pk_out = col_map[primary_key]
            patch[pk_out] = mlwh[primary_key]
            return patch
        return None

    def pretty(self):
        fmt = io.StringIO()
        fmt.write(f"\n{bold(self.data_id)}\n")

        max_col_width = 0
        max_mlwh_val_width = 0
        mlwh_values = []
        tolqc_values = []
        for col in self.differing_columns:
            if (x := len(col)) > max_col_width:
                max_col_width = x
            mlwh_v, mlwh_style = field_style(col, self.mlwh[col])
            if (y := len(mlwh_v)) > max_mlwh_val_width:
                max_mlwh_val_width = y
            mlwh_values.append((mlwh_v, mlwh_style))
            tolqc_values.append(field_style(col, self.tolqc[col]))

        fmt.write(f"  {'':{max_col_width}}  {'MLWH':{max_mlwh_val_width}}  ToLQC\n")
        for col, (mlwh_v, mlwh_style), (tolqc_v, tolqc_style) in zip(
            self.differing_columns,
            mlwh_values,
            tolqc_values,
            strict=True,
        ):
            mlwh_fmt = f"{mlwh_v:{max_mlwh_val_width}}"
            fmt.write(
                f"  {col:>{max_col_width}}  {mlwh_style(mlwh_fmt)}"
                f"  {tolqc_style(tolqc_v)}\n"
            )

        return fmt.getvalue()


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
        self.differing_columns.append(m.differing_columns)

    def arrow_table(self):
        return pyarrow.Table.from_pydict(
            {
                "data_id": pyarrow.array(self.data_id),
                "mlwh_hash": pyarrow.array(self.mlwh_hash),
                "tolqc_hash": pyarrow.array(self.tolqc_hash),
                "differing_columns": pyarrow.array(self.differing_columns),
            }
        )

    def store(self, conn):
        count = len(self.data_id)
        if count == 0:
            logging.info("No new differences found")
            return
        logging.info(f"Found {count} new differences")
        arrow_table__ = self.arrow_table()  # noqa: F841
        conn.sql(
            "INSERT INTO diff_store SELECT *, current_timestamp FROM arrow_table__"
        )


@click.command
@click_options.tolqc_alias
@click_options.tolqc_url
@click_options.api_token
@click_options.log_level
@click_options.diff_mlwh_duckdb
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
    "--new/--all",
    "show_new_diffs",
    help="""Print the most recently detected mismatches to STDOUT
      instead of the default of printing all stored mismatches.
      Overridden by the --since or --today options""",
    default=False,
    show_default=True,
)
@click.option(
    "--today",
    "since",
    flag_value=click_options.TODAY,
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
    "--show-classes",
    flag_value=True,
    help="""Show the list of differences grouped by the names of columns
      which differ and their counts""",
)
@click.option(
    "--class",
    "column_class",
    help="""Show the differences with this combination of columns which differ.
      (See output from --show-classes)""",
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(
        ["PRETTY", "NDJSON"],
        case_sensitive=False,
    ),
    help="""Output differences found in either 'PRETTY'
      (human readable) or 'NDJSON' format.
      Defaults to 'PRETTY' if stdout is a terminal, else 'NDJSON'""",
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
    diff_mlwh_duckdb,
    mlwh_ndjson,
    show_new_diffs,
    show_classes,
    column_class,
    output_format,
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
    tqc = tolqc_client.TolClient(tolqc_url, api_token, tolqc_alias)
    if column_class:
        column_class = column_class.split(",")

    if not output_format:
        output_format = "PRETTY" if sys.stdout.isatty() else "NDJSON"

    run_mlwh_diff(
        tqc,
        diff_mlwh_duckdb,
        mlwh_ndjson,
        show_new_diffs,
        show_classes,
        column_class,
        output_format,
        since,
        table,
        update,
    )


def run_mlwh_diff(
    tqc,
    diff_mlwh_duckdb=None,
    mlwh_ndjson=None,
    show_new_diffs=False,
    show_classes=False,
    column_class=None,
    output_format="NDJSON",
    since=None,
    table=None,
    update=False,
):
    conn = duckdb.connect(str(diff_mlwh_duckdb))

    create_diff_db(conn, tqc, mlwh_ndjson, update)

    if show_classes:
        show_diff_classes(conn)
        return

    itr = fetch_stored_diffs(conn, since, show_new_diffs, column_class)
    if table:
        col_map = table_map().get(table)
        if not col_map:
            exit(f"No column map for table '{table}'")
        for m in itr:
            if patch := m.get_patch_for_table(table, col_map):
                sys.stdout.write(ndjson_row(patch))
    else:
        if output_format == "PRETTY":
            if sys.stdout.isatty():
                click.echo_via_pager(pretty_diff_iterator(itr))
            else:
                # Prevent empty emails being sent from cron jobs.
                # echo_via_pager() prints a newline if there are no diffs to
                # print, so avoid it if not attached to a TTY.
                for txt in pretty_diff_iterator(itr):
                    click.echo(txt)
        else:
            for m in itr:
                sys.stdout.write(ndjson_row(m.differences_dict))


def pretty_diff_iterator(itr):
    n = 0
    for m in itr:
        n += 1
        yield m.pretty()

    if n:
        yield f"\n{bold(n)} mismatch{'es' if n > 1 else ''} between MLWH and ToLQC"


def create_diff_db(conn, tqc, mlwh_ndjson=None, update=False):
    tables = {x[0] for x in conn.execute("SHOW TABLES").fetchall()}

    do_compare_tables = update
    if "diff_store" not in tables:
        do_compare_tables = True
        create_diff_store(conn)

    if "update_log" not in tables:
        conn.execute("CREATE TABLE update_log(updated_at TIMESTAMPTZ)")

    # Fetch the current MLWH data from ToLQC
    if update or "tolqc" not in tables:
        do_compare_tables = True
        tolqc_tmp = NamedTemporaryFile("r", prefix="tolqc_", suffix=".ndjson")
        logging.info(
            f"Downloading current data from {tqc.tolqc_alias} into {tolqc_tmp.name}"
        )
        tqc.download_file("report/mlwh-data?format=NDJSON", tolqc_tmp.name)
        load_table_from_json(conn, "tolqc", tolqc_tmp.name)

    if update or "mlwh" not in tables:
        do_compare_tables = True
        if mlwh_ndjson and mlwh_ndjson.exists():
            logging.info(f"Loading MLWH data from {mlwh_ndjson}")
            load_table_from_json(conn, "mlwh", str(mlwh_ndjson))
        else:
            mlwh_tmp = NamedTemporaryFile(
                "w", prefix="mlwh_", suffix=".ndjson", delete_on_close=False
            )
            logging.info(f"Downloading data from MLWH into {mlwh_tmp.name}")
            fetch_mlwh_seq_data_to_file(tqc, mlwh_tmp)
            load_table_from_json(conn, "mlwh", mlwh_tmp.name)

    if do_compare_tables:
        conn.execute("INSERT INTO update_log VALUES (current_timestamp)")
        ds = DiffStore()
        for m in compare_tables(conn):
            ds.add(m)
        ds.store(conn)


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
    fetch_mlwh_seq_data.write_mlwh_data_to_filehandle(
        mlwh, tqc.list_project_study_ids(), mlwh_ndjson
    )
    mlwh_ndjson.close()


def show_diff(frst, scnd):
    for key, frst_v in frst.items():
        scnd_v = scnd[key]
        if frst_v != scnd_v:
            click.echo(f"  {key}: {frst_v!r} | {scnd[key]!r}")


def fetch_stored_diffs(conn, since=None, show_new_diffs=False, column_class=None):
    args = []
    where = []
    if since:
        where.append("ds.found_at >= ?")
        args.append(since)
    elif show_new_diffs:
        where.append("ds.found_at >= (SELECT MAX(updated_at) FROM update_log)")

    if column_class:
        where.append("ds.differing_columns = ?")
        args.append(column_class)

    sql = inspect.cleandoc("""
        SELECT ds.data_id
          , mlwh
          , tolqc
          , ds.mlwh_hash
          , ds.tolqc_hash
          , ds.differing_columns
        FROM diff_store ds
        JOIN mlwh USING (data_id)
        JOIN tolqc USING (data_id)
    """)

    if where:
        sql += "\nWHERE " + "\n  AND ".join(where)

    sql += "\nORDER BY ds.data_id"

    debug_sql(sql)
    conn.execute(sql, args)

    while diff := conn.fetchone():
        yield Mismatch(*diff)


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
    Creates two tables from the two tables `mlwh` and `tolqc`. The tables
    contain the `data_id`, an MD5 hash of the entire row cast to VARCHAR, and
    the row itself as a DuckDB STRUCT.

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
          , mlwh AS mlwh_struct
          , tolqc AS tolqc_struct
          , mlwh_h.hash AS mlwh_hash
          , tolqc_h.hash AS tolqc_hash
        FROM mlwh_h JOIN tolqc_h
          ON mlwh_h.data_id = tolqc_h.data_id
          AND mlwh_h.hash != tolqc_h.hash
        JOIN mlwh
          ON mlwh_h.data_id = mlwh.data_id
        JOIN tolqc
          ON tolqc_h.data_id = tolqc.data_id
        ANTI JOIN diff_store AS mds
          ON mlwh_h.hash = mds.mlwh_hash
        ANTI JOIN diff_store AS qds
          ON tolqc_h.hash = qds.tolqc_hash
        ORDER BY mlwh_h.data_id
    """)
    debug_sql(sql)
    conn.execute(sql)

    while row := conn.fetchone():
        yield Mismatch(*row)


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
        CREATE TABLE diff_store(
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

    table_map = {
        "file": {"data_id": "data.id"},
        "pacbio_run_metrics": {"run_id": "pacbio_run_metrics.id"},
        "platform": {"run_id": "run.id"},
    }
    for name, tbl, col in name_table_column(query):
        # if col.name == "library_type_id":
        #     click.echo(f"{col.name = } {col.foreign_keys = }", err=True)
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
        # {
        #     "name": "sample_name",
        #     "type": String(),
        #     "aliased": False,
        #     "expr": "<sqlalchemy.sql.elements.Label at 0x10bf3fe80; sample_name>",
        #     "entity": "<class 'tolqc.sample_data_models.Sample'>",
        # }
        name = desc["name"]
        if name == "supplier_name":
            continue
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
