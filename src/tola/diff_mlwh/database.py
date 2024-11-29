import inspect
import logging
from tempfile import NamedTemporaryFile

import click
import duckdb

from tola.diff_mlwh.column_definitions import json_cols, table_cols
from tola.diff_mlwh.diff_store import DiffStore, Mismatch
from tola.pretty import bold, dim


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
    since=None,
):
    update = False
    if mlwh_ndjson or not diff_mlwh_duckdb.exists():
        update = True
    conn = duckdb.connect(
        database=str(diff_mlwh_duckdb),
        read_only=not (update or add_reason_dict or reason_action),
    )

    if update:
        create_diff_db_tables(conn)
        load_new_data(conn, tqc, mlwh_ndjson)
        find_diffs(conn)

    if show_classes:
        show_diff_classes(conn)
        return

    if add_reason_dict:
        load_reason_dict_entry(conn, add_reason_dict)

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

    return diffs


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


def create_diff_db_tables(conn):
    tables = {x[0] for x in conn.execute("SHOW TABLES").fetchall()}

    if "diff_store" not in tables:
        create_diff_store(conn)

    if "update_log" not in tables:
        conn.execute("CREATE TABLE update_log(updated_at TIMESTAMPTZ)")

    if "diff_reason" not in tables:
        create_reasons_tables(conn)

    create_or_update_macros_and_views(conn)


def load_new_data(conn, tqc, mlwh_ndjson=None):
    # Fetch the current MLWH data from ToLQC
    tolqc_tmp = NamedTemporaryFile("r", prefix="tolqc_", suffix=".ndjson")
    logging.info(
        f"Downloading current data from {tqc.tolqc_alias} into {tolqc_tmp.name}"
    )
    tqc.download_file("report/mlwh-data?format=NDJSON", tolqc_tmp.name)
    load_table_from_json(conn, "tolqc", tolqc_tmp.name)

    load_table_from_json(conn, "mlwh", str(mlwh_ndjson))


def find_diffs(conn):
    conn.execute("INSERT INTO update_log VALUES (current_timestamp)")
    ds = DiffStore()
    for m in compare_tables(conn):
        ds.add(m)
    store_diffs(conn, ds)


def store_diffs(conn, ds):
    count = len(ds.data_id)
    if count == 0:
        logging.info("No new differences found")
        return
    logging.info(f"Found {count} new differences")
    arrow_table__ = ds.arrow_table()  # noqa: F841
    conn.execute(
        "INSERT INTO diff_store SELECT *, current_timestamp FROM arrow_table__"
    )


def load_table_from_json(conn, name, file):
    logging.info(f"Loading {file} into {name} table")

    # NDJSON from MLWH has different number of columns for Illumina and PacBio
    # data.  To create the same table structure for the `mlwh` and `tolqc`
    # tables we provide the column mapping.
    conn.execute(f"CREATE OR REPLACE TABLE {name}({table_cols()})")
    conn.execute(
        f"INSERT INTO {name} FROM read_json(?, columns = {{{json_cols()}}})", (file,)
    )


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
