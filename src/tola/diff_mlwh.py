import inspect
import io
import logging
import pathlib
import sys
from contextlib import contextmanager
from tempfile import NamedTemporaryFile
from typing import Any

import click
import duckdb
import pyarrow

from tola import click_options, db_connection, fetch_mlwh_seq_data, tolqc_client
from tola.ndjson import ndjson_row
from tola.pretty import bg_green, bg_red, bold, colour_pager, dim, field_style


class Mismatch:
    """Stores a row from the diff query"""

    __slots__ = (
        "data_id",
        "mlwh",
        "tolqc",
        "mlwh_hash",
        "tolqc_hash",
        "differing_columns",
        "reasons",
    )

    def __init__(
        self,
        data_id: str,
        mlwh: dict[str, Any],
        tolqc: dict[str, Any],
        mlwh_hash: str,
        tolqc_hash: str,
        differing_columns: list[str] = None,
        reasons: list[str] = None,
    ):
        self.data_id = data_id
        self.mlwh = mlwh
        self.tolqc = tolqc
        self.mlwh_hash = mlwh_hash
        self.tolqc_hash = tolqc_hash
        self.reasons = reasons
        if differing_columns:
            self.differing_columns = differing_columns
        else:
            self._build_differing_columns()

    @property
    def diff_class(self) -> list[str]:
        return ",".join(self.differing_columns)

    def differences_dict(self, show_columns):
        dd = {
            "data_id": self.data_id,
            "reasons": rsns if (rsns := self.reasons) else [],
        }
        col_names = set(self.differing_columns)
        if show_columns:
            col_names |= show_columns
        for col in self.mlwh:
            if col in col_names:
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

    def pretty(self, show_columns=None):
        fmt = io.StringIO()
        fmt.write(f"\n{bold(self.data_id)}")
        if sn := self.mlwh.get("sample_name"):
            fmt.write(f"  {sn}")
        if self.reasons:
            fmt.write(f"  ({' & '.join(bold(x) for x in self.reasons)})")
        fmt.write("\n")

        diff_set = set(self.differing_columns)

        # Build a set of column names which will be shown
        if show_columns:
            col_names = []
            if "ALL" in show_columns:
                for col, mlwh_v in self.mlwh.items():
                    # Skip columns where both values are None
                    # Avoids showing all the empty PacBio columns for Illumina
                    if mlwh_v is not None or self.tolqc[col] is not None:
                        col_names.append(col)
            else:
                show_set = diff_set | show_columns
                for col in self.mlwh:
                    if col in show_set:
                        col_names.append(col)
        else:
            col_names = self.differing_columns

        # Calculate the layout of the output
        max_col_width = 0
        max_mlwh_val_width = 0
        mlwh_values = []
        tolqc_values = []
        for col in col_names:
            if (x := len(col)) > max_col_width:
                max_col_width = x
            mlwh_v, mlwh_style = field_style(col, self.mlwh[col])
            if (y := len(mlwh_v)) > max_mlwh_val_width:
                max_mlwh_val_width = y
            mlwh_values.append((mlwh_v, mlwh_style))
            tolqc_values.append(field_style(col, self.tolqc[col]))

        # Create the pretty output
        fmt.write(f"  {'':{max_col_width}}  {'MLWH':{max_mlwh_val_width}}  ToLQC\n")
        for col, (mlwh_v, mlwh_style), (tolqc_v, tolqc_style) in zip(
            col_names,
            mlwh_values,
            tolqc_values,
            strict=True,
        ):
            pad = " " * (max_mlwh_val_width - len(mlwh_v))
            mlwh_fmt = mlwh_style(mlwh_v)
            tolqc_fmt = tolqc_style(tolqc_v)
            if show_columns:
                # When extra columns have been requested, highlight matching
                # and differing values.
                if mlwh_v == tolqc_v:
                    mlwh_fmt = bg_green(mlwh_fmt)
                    tolqc_fmt = bg_green(tolqc_fmt)
                else:
                    if mlwh_v != "null":
                        mlwh_fmt = bg_red(mlwh_fmt)
                    if tolqc_v != "null":
                        tolqc_fmt = bg_red(tolqc_fmt)

            fmt.write(f"  {col:>{max_col_width}}  {mlwh_fmt}{pad}  {tolqc_fmt}\n")

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
        conn.execute(
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
    "--show-reason-dict",
    flag_value=True,
    help="""Show the dictionary of reasons for differences between the MLWH and
      ToLQC databases.""",
)
@click.option(
    "--add-reason-dict",
    nargs=2,
    metavar=("REASON", "DESCRIPTION"),
    help="""Add a reason and its description to the dictionary of reasons""",
)
@click.option(
    "--reason",
    metavar="REASON",
    help="""Show the differences tagged with this reason.
      The value "NONE" will show any differences not tagged with a reason.
      (See output from --show-reason-dict for the list of reasons)""",
)
@click.option(
    "--store-reason",
    metavar="REASON",
    help="Store the reason for the list of `data_id` supplied",
)
@click.option(
    "--delete-reason",
    metavar="REASON",
    help="Delete the reason for the list of `data_id` supplied",
)
@click.argument(
    "data_id_list",
    metavar="DATA_ID_LIST",
    nargs=-1,
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
    "--show",
    "show_columns",
    multiple=True,
    help="""Columns to show. Can be specified multiple time or as a comma
      separated list. The value "ALL" will show all columns, except those
      which are null in both MLWH and ToLQC.""",
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
    show_reason_dict,
    add_reason_dict,
    reason,
    store_reason,
    delete_reason,
    data_id_list,
    output_format,
    show_columns,
    since,
    table,
    update,
):
    """
    Compare the contents of the MLWH to the ToLQC database

    DATA_ID_LIST is a list of `data_id` values to act on.
    """

    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(message)s",
        force=True,
    )
    tqc = tolqc_client.TolClient(tolqc_url, api_token, tolqc_alias)

    if column_class:
        column_class = column_class.split(",")

    if show_columns:
        sc = []
        for spec in show_columns:
            if spec.upper() == "ALL":
                sc = ["ALL"]
                break
            sc.extend(spec.split(","))
        show_columns = set(sc)
    else:
        show_columns = None

    reason_action = None
    if store_reason:
        reason_action = "STORE"
        reason = store_reason
    elif delete_reason:
        reason_action = "DELETE"
        reason = delete_reason

    if not output_format:
        output_format = "PRETTY" if sys.stdout.isatty() else "NDJSON"

    run_mlwh_diff(
        tqc,
        diff_mlwh_duckdb,
        mlwh_ndjson,
        show_new_diffs,
        show_classes,
        column_class,
        show_reason_dict,
        add_reason_dict,
        reason,
        reason_action,
        data_id_list,
        output_format,
        show_columns,
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
    show_reason_dict=False,
    add_reason_dict=None,
    reason=None,
    reason_action=None,
    data_id_list=None,
    output_format="NDJSON",
    show_columns=None,
    since=None,
    table=None,
    update=False,
):
    if not diff_mlwh_duckdb.exists():
        update = True
    conn = duckdb.connect(
        database=str(diff_mlwh_duckdb),
        read_only=not (update or add_reason_dict or reason_action),
    )

    if update:
        with transaction(conn) as crsr:
            create_diff_db(crsr, tqc, mlwh_ndjson)

    if show_classes:
        show_diff_classes(conn)
        return

    if add_reason_dict:
        with transaction(conn) as crsr:
            load_reason_dict_entry(crsr, add_reason_dict)

    if show_reason_dict or add_reason_dict:
        show_reason_dict_contents(conn)
        return

    if reason_action and data_id_list:
        if reason_action == "STORE":
            store_reasons(conn, reason, data_id_list)
        elif reason_action == "DELETE":
            count = delete_reasons(conn, reason, data_id_list)
            click.echo(f"Deleted {bold(count)} reason tags")
            return

    diffs = fetch_stored_diffs(
        conn,
        since,
        show_new_diffs,
        column_class,
        reason,
        data_id_list,
    )
    conn.close()

    if table:
        col_map = table_map().get(table)
        if not col_map:
            exit(f"No column map for table '{table}'")
        for m in diffs:
            if patch := m.get_patch_for_table(table, col_map):
                sys.stdout.write(ndjson_row(patch))
    else:
        if output_format == "PRETTY":
            if sys.stdout.isatty():
                colour_pager(pretty_diff_iterator(diffs, show_columns))
            else:
                # Prevent empty emails being sent from cron jobs.
                # echo_via_pager() prints a newline if there are no diffs to
                # print, so avoid it if not attached to a TTY.
                for txt in pretty_diff_iterator(diffs, show_columns):
                    click.echo(txt)
        else:
            for m in diffs:
                sys.stdout.write(ndjson_row(m.differences_dict(show_columns)))


@contextmanager
def transaction(conn):
    crsr = conn.cursor()
    crsr.execute("BEGIN TRANSACTION")
    try:
        yield crsr
    except duckdb.Error as err:
        crsr.rollback()
        raise err
    crsr.commit()


def load_reason_dict_ndjson(conn, edit_reason_dict):
    file = "/dev/stdin" if edit_reason_dict == "-" else edit_reason_dict
    sql = inspect.cleandoc("""
        INSERT OR REPLACE INTO reason_dict
        FROM read_json(?, columns = {
          reason: 'VARCHAR', description: 'VARCHAR'
        })
    """)
    debug_sql(sql)
    conn.execute(sql, (file,))


def load_reason_dict_entry(conn, reason_dict_line):
    sql = inspect.cleandoc("""
        INSERT OR REPLACE INTO reason_dict
        VALUES (?,?)
    """)
    debug_sql(sql)
    conn.execute(sql, reason_dict_line)


def store_reasons(conn, reason, data_id_list):
    sql = inspect.cleandoc("""
        INSERT OR IGNORE INTO diff_reason(data_id, reason)
        FROM (SELECT unnest(list_transform(?, x -> (x, ?)), recursive := true))
    """)
    debug_sql(sql)
    conn.execute(sql, (data_id_list, reason))


def delete_reasons(conn, reason, data_id_list):
    template = inspect.cleandoc("""
        {} FROM diff_reason
        WHERE list_contains(?, data_id)
          AND reason = ?
    """)

    # Count the number of rows which will be deleted, then delete them.
    conn.execute(template.format("SELECT COUNT(*)"), (data_id_list, reason))
    (row_count,) = conn.fetchone()
    conn.execute(template.format("DELETE"), (data_id_list, reason))

    return row_count


def pretty_diff_iterator(itr, show_columns=None):
    n = 0
    for m in itr:
        n += 1
        yield m.pretty(show_columns)

    if n:
        yield f"\n{bold(n)} mismatch{'es' if n > 1 else ''} between MLWH and ToLQC"


def create_diff_db(conn, tqc, mlwh_ndjson=None):
    tables = {x[0] for x in conn.execute("SHOW TABLES").fetchall()}

    if "diff_store" not in tables:
        create_diff_store(conn)

    if "update_log" not in tables:
        conn.execute("CREATE TABLE update_log(updated_at TIMESTAMPTZ)")

    if "diff_reason" not in tables:
        create_reasons_tables(conn)

    # Fetch the current MLWH data from ToLQC
    tolqc_tmp = NamedTemporaryFile("r", prefix="tolqc_", suffix=".ndjson")
    logging.info(
        f"Downloading current data from {tqc.tolqc_alias} into {tolqc_tmp.name}"
    )
    tqc.download_file("report/mlwh-data?format=NDJSON", tolqc_tmp.name)
    load_table_from_json(conn, "tolqc", tolqc_tmp.name)

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

    conn.execute("INSERT INTO update_log VALUES (current_timestamp)")
    ds = DiffStore()
    for m in compare_tables(conn):
        ds.add(m)
    ds.store(conn)

    create_or_update_macros_and_views(conn)


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


def fetch_stored_diffs(
    conn,
    since=None,
    show_new_diffs=False,
    column_class=None,
    reason=None,
    data_id_list=None,
):
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

    if reason:
        if reason.upper() == "NONE":
            where.append("drl.reasons IS NULL")
        else:
            where.append("list_contains(drl.reasons, ?)")
            args.append(reason)

    if data_id_list:
        where.append("list_contains(?, ds.data_id)")
        args.append(data_id_list)

    sql = inspect.cleandoc("""
        WITH drl AS (
            SELECT data_id
              , array_agg(reason ORDER BY reason) AS reasons
            FROM diff_reason
            GROUP BY data_id
        )
        SELECT ds.data_id
          , mlwh
          , tolqc
          , ds.mlwh_hash
          , ds.tolqc_hash
          , ds.differing_columns
          , drl.reasons
        FROM diff_store ds
        JOIN mlwh USING (data_id)
        JOIN tolqc USING (data_id)
        LEFT JOIN drl USING (data_id)
    """)

    if where:
        sql += "\nWHERE " + "\n  AND ".join(where)

    sql += "\nORDER BY ds.data_id"

    debug_sql(sql)
    conn.execute(sql, args)

    return [Mismatch(*diff) for diff in conn.fetchall()]


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


def show_reason_dict_contents(conn):
    sql = inspect.cleandoc("""
        SELECT reason_dict
        FROM reason_dict
        ORDER BY reason
    """)
    debug_sql(sql)
    conn.execute(sql)

    reasons = [x[0] for x in conn.fetchall()]
    if not reasons:
        return
    max_name = max(len(x["reason"]) for x in reasons)
    for rd in reasons:
        click.echo(f" {rd['reason']:>{max_name}}:  {rd['description'] or dim('null')}")


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


def create_reasons_tables(conn):
    sql = inspect.cleandoc(
        """
        CREATE TABLE reason_dict(
            reason VARCHAR PRIMARY KEY
            , description VARCHAR
        )
        """
    )
    debug_sql(sql)
    conn.execute(sql)

    sql = inspect.cleandoc(
        """
        CREATE TABLE diff_reason(
            data_id VARCHAR
            , reason VARCHAR REFERENCES reason_dict (reason)
            , PRIMARY KEY (data_id, reason)
        )
        """
    )
    debug_sql(sql)
    conn.execute(sql)


def table_map():
    """
    Built using the script `scripts/make_table_map.py`, then hand edited.
    """

    return {
        "file": {
            "data_id": "data.id",
            "remote_path": "remote_path",
        },
        "pacbio_run_metrics": {
            "run_id": "pacbio_run_metrics.id",
            "movie_minutes": "movie_minutes",
            "binding_kit": "binding_kit",
            "sequencing_kit": "sequencing_kit",
            "sequencing_kit_lot_number": "sequencing_kit_lot_number",
            "cell_lot_number": "cell_lot_number",
            "include_kinetics": "include_kinetics",
            "loading_conc": "loading_conc",
            "control_num_reads": "control_num_reads",
            "control_read_length_mean": "control_read_length_mean",
            "control_concordance_mean": "control_concordance_mean",
            "control_concordance_mode": "control_concordance_mode",
            "local_base_rate": "local_base_rate",
            "polymerase_read_bases": "polymerase_read_bases",
            "polymerase_num_reads": "polymerase_num_reads",
            "polymerase_read_length_mean": "polymerase_read_length_mean",
            "polymerase_read_length_n50": "polymerase_read_length_n50",
            "insert_length_mean": "insert_length_mean",
            "insert_length_n50": "insert_length_n50",
            "unique_molecular_bases": "unique_molecular_bases",
            "productive_zmws_num": "productive_zmws_num",
            "p0_num": "p0_num",
            "p1_num": "p1_num",
            "p2_num": "p2_num",
            "adapter_dimer_percent": "adapter_dimer_percent",
            "short_insert_percent": "short_insert_percent",
            "hifi_read_bases": "hifi_read_bases",
            "hifi_num_reads": "hifi_num_reads",
            "hifi_read_length_mean": "hifi_read_length_mean",
            "hifi_read_quality_median": "hifi_read_quality_median",
            "hifi_number_passes_mean": "hifi_number_passes_mean",
            "hifi_low_quality_read_bases": "hifi_low_quality_read_bases",
            "hifi_low_quality_num_reads": "hifi_low_quality_num_reads",
            "hifi_low_quality_read_length_mean": "hifi_low_quality_read_length_mean",
            "hifi_low_quality_read_quality_median": "hifi_low_quality_read_quality_median",
            "hifi_barcoded_reads": "hifi_barcoded_reads",
            "hifi_bases_in_barcoded_reads": "hifi_bases_in_barcoded_reads",
        },
        "platform": {
            # platform.id is an auto-incremented integer, so we don't know what it is
            "run_id": "run.id",
            "platform_type": "name",
            "instrument_model": "model",
        },
        "data": {
            "data_id": "data.id",
            "study_id": "study_id",
            "tag_index": "tag_index",
            "lims_qc": "lims_qc",
            "qc_date": "date",
            "tag1_id": "tag1_id",
            "tag2_id": "tag2_id",
        },
        "sample": {
            "sample_name": "sample.id",
            "tol_specimen_id": "specimen.id",  # Added by hand
            "biosample_accession": "accession_id",
        },
        "specimen": {
            "tol_specimen_id": "specimen.id",
            "scientific_name": "species.id",  # Added by hand
            "biospecimen_accession": "accession_id",
        },
        "species": {
            "scientific_name": "species.id",
            "taxon_id": "taxon_id",
        },
        "run": {
            "run_id": "run.id",
            "instrument_name": "instrument_name",
            "lims_run_id": "lims_id",
            "element": "element",
            "run_start": "start",
            "run_complete": "complete",
            "plex_count": "plex_count",
        },
        "library": {
            "pipeline_id_lims": "library_type_id",
            "library_id": "library.id",
        },
    }

    return table_map


def column_definitions():
    """
    Returns column definitions for SQL CREATE TABLE and DuckDB JSON parsing.
    """

    # Built using the script `scripts/make_table_map.py`
    col_defs = {
        "data_id": "VARCHAR",
        "study_id": "INTEGER",
        "sample_name": "VARCHAR",
        "tol_specimen_id": "VARCHAR",
        "biosample_accession": "VARCHAR",
        "biospecimen_accession": "VARCHAR",
        "scientific_name": "VARCHAR",
        "taxon_id": "INTEGER",
        "platform_type": "VARCHAR",
        "instrument_model": "VARCHAR",
        "instrument_name": "VARCHAR",
        "pipeline_id_lims": "VARCHAR",
        "run_id": "VARCHAR",
        "tag_index": "VARCHAR",
        "lims_run_id": "VARCHAR",
        "element": "VARCHAR",
        "run_start": "TIMESTAMPTZ",
        "run_complete": "TIMESTAMPTZ",
        "plex_count": "INTEGER",
        "lims_qc": "VARCHAR",
        "qc_date": "TIMESTAMPTZ",
        "tag1_id": "VARCHAR",
        "tag2_id": "VARCHAR",
        "library_id": "VARCHAR",
        "movie_minutes": "INTEGER",
        "binding_kit": "VARCHAR",
        "sequencing_kit": "VARCHAR",
        "sequencing_kit_lot_number": "VARCHAR",
        "cell_lot_number": "VARCHAR",
        "include_kinetics": "VARCHAR",
        "loading_conc": "FLOAT",
        "control_num_reads": "INTEGER",
        "control_read_length_mean": "FLOAT",
        "control_concordance_mean": "FLOAT",
        "control_concordance_mode": "FLOAT",
        "local_base_rate": "FLOAT",
        "polymerase_read_bases": "BIGINT",
        "polymerase_num_reads": "INTEGER",
        "polymerase_read_length_mean": "FLOAT",
        "polymerase_read_length_n50": "INTEGER",
        "insert_length_mean": "FLOAT",
        "insert_length_n50": "INTEGER",
        "unique_molecular_bases": "BIGINT",
        "productive_zmws_num": "INTEGER",
        "p0_num": "INTEGER",
        "p1_num": "INTEGER",
        "p2_num": "INTEGER",
        "adapter_dimer_percent": "FLOAT",
        "short_insert_percent": "FLOAT",
        "hifi_read_bases": "BIGINT",
        "hifi_num_reads": "INTEGER",
        "hifi_read_length_mean": "INTEGER",
        "hifi_read_quality_median": "INTEGER",
        "hifi_number_passes_mean": "FLOAT",
        "hifi_low_quality_read_bases": "BIGINT",
        "hifi_low_quality_num_reads": "INTEGER",
        "hifi_low_quality_read_length_mean": "INTEGER",
        "hifi_low_quality_read_quality_median": "INTEGER",
        "hifi_barcoded_reads": "INTEGER",
        "hifi_bases_in_barcoded_reads": "BIGINT",
        "remote_path": "VARCHAR",
    }

    table_cols = "\n, ".join(
        f"{n} {t} PRIMARY KEY" if n == "data_id" else f"{n} {t}"
        for n, t in col_defs.items()
    )

    json_cols = "\n, ".join(f"{n}: '{t}'" for n, t in col_defs.items())

    return table_cols, json_cols


def create_or_update_macros_and_views(conn):
    """Useful DuckDB macros and views"""

    for sql_def in (
        # Implements speciesops directory hash.
        # e.g. taxon_hash(9627) produces '2/e/9/7/6/a'
        """
        MACRO taxon_hash(taxon_id) AS
          md5(taxon_id::VARCHAR)[:6].split('').array_to_string('/')
        """,
        # Converts species scientific name to hierarchy_name
        r"""
        MACRO species_hn(species) AS
            regexp_replace(species, '\W+', '_', 'g').trim('_')
        """,
        # Uses taxon_hash() and species_hn() to build path to lustre directory
        """
        MACRO species_lustre(taxon_id, species) AS
          CONCAT_WS('/'
            , '/lustre/scratch122/tol/data'
            , taxon_hash(taxon_id)
            , species_hn(species))
        """,
        # Creates a view of the tolqc table showing the species directories
        """
        VIEW species_dir AS
          SELECT DISTINCT scientific_name AS species_id
            , taxon_id
            , species_lustre(taxon_id, scientific_name) AS directory
          FROM tolqc
          WHERE taxon_id IS NOT NULL
            AND scientific_name IS NOT NULL
          ORDER BY ALL
        """,
    ):
        conn.execute("CREATE OR REPLACE " + sql_def)
