import inspect
import logging
from tempfile import NamedTemporaryFile

import click
import duckdb

from tola.diff_mlwh.column_definitions import json_cols, table_cols
from tola.diff_mlwh.diff_store import DiffStore, Mismatch
from tola.pretty import dim

log = logging.getLogger(__name__)


class MLWHDiffDB:
    def __init__(self, path, write_flag=True):
        if not path.exists():
            write_flag = True
        self.conn = duckdb.connect(str(path), read_only=not write_flag)
        if write_flag:
            self.create_diff_db_tables()

    def execute(self, sql, params=None):
        log.debug(
            f"{sql};\n"
            + "".join(f"  p{i + 1}: {p!r}\n" for i, p in enumerate(params or ()))
        )
        return self.conn.execute(sql, params)

    def update(self, tqc, mlwh_ndjson):
        self.load_new_data(tqc, mlwh_ndjson)
        self.find_diffs()

    def load_reason_dict_entry(self, reason_dict_row: tuple[str, str]):
        sql = inspect.cleandoc("""
            INSERT OR REPLACE INTO reason_dict
            VALUES (?,?)
        """)
        self.execute(sql, reason_dict_row)

    def load_reason_dict_ndjson(self, reason_dict_ndjson):
        file = "/dev/stdin" if reason_dict_ndjson == "-" else reason_dict_ndjson
        sql = inspect.cleandoc("""
            INSERT OR REPLACE INTO reason_dict
            FROM read_json(?, columns = {
              reason: 'VARCHAR', description: 'VARCHAR'
            })
        """)
        self.execute(sql, (file,))

    def store_reasons(self, reason, data_id_list):
        sql = inspect.cleandoc("""
            INSERT OR IGNORE INTO diff_reason(data_id, reason)
            FROM (SELECT unnest(list_transform(?, x -> (x, ?)), recursive := true))
        """)
        self.execute(sql, (data_id_list, reason))

    def delete_reasons(self, reason, data_id_list):
        template = inspect.cleandoc("""
            {} FROM diff_reason
            WHERE list_contains(?, data_id)
              AND reason = ?
        """)

        # Count the number of rows which will be deleted, then delete them.
        crsr = self.execute(template.format("SELECT COUNT(*)"), (data_id_list, reason))
        (row_count,) = crsr.fetchone()
        self.execute(template.format("DELETE"), (data_id_list, reason))

        return row_count

    def create_diff_db_tables(self):
        tables = {x[0] for x in self.execute("SHOW TABLES").fetchall()}

        if "diff_store" not in tables:
            self.create_diff_store()

        if "update_log" not in tables:
            self.execute("CREATE TABLE update_log(updated_at TIMESTAMPTZ)")

        if "diff_reason" not in tables:
            self.create_reasons_tables()

        for name in ("tolqc", "mlwh"):
            if name not in tables:
                self.create_data_table(name)

        self.create_or_update_macros_and_views()

    def load_new_data(self, tqc, mlwh_ndjson):
        # Fetch the current MLWH data from ToLQC
        tolqc_tmp = NamedTemporaryFile("r", prefix="tolqc_", suffix=".ndjson")  # noqa: SIM115
        log.info(
            f"Downloading current data from {tqc.tolqc_alias} into {tolqc_tmp.name}"
        )
        tqc.download_file("report/mlwh-data?format=NDJSON", tolqc_tmp.name)

        self.load_table_from_json("tolqc", tolqc_tmp.name)
        self.load_table_from_json("mlwh", str(mlwh_ndjson))

    def find_diffs(self):
        self.execute("INSERT INTO update_log VALUES (current_timestamp)")
        ds = DiffStore()
        for m in self.compare_tables():
            ds.add(m)
        self.store_diffs(ds)

    def store_diffs(self, ds):
        count = len(ds.data_id)
        if count == 0:
            log.info("No new differences found")
            return
        log.info(f"Found {count} new differences")
        arrow_table = ds.arrow_table()  # noqa: F841
        sql = "INSERT INTO diff_store SELECT *, current_timestamp FROM arrow_table"
        log.debug(sql)
        self.conn.execute(sql)

    def create_data_table(self, name):
        self.execute(f"CREATE OR REPLACE TABLE {name}({table_cols()})")

    def load_table_from_json(self, name, file):
        log.info(f"Loading {file} into {name} table")
        self.create_data_table(name)

        # NDJSON from MLWH has different number of columns for Illumina and PacBio
        # data.  To create the same table structure for the `mlwh` and `tolqc`
        # tables we provide the column mapping.
        self.execute(
            f"INSERT INTO {name} FROM read_json(?, columns = {{{json_cols()}}})",
            (file,),
        )

    def fetch_stored_diffs(
        self,
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

        crsr = self.execute(sql, args)

        return [Mismatch(*diff) for diff in crsr.fetchall()]

    def show_diff_classes(self):
        sql = inspect.cleandoc("""
            SELECT count(*) AS n
              , differing_columns
            FROM diff_store
            GROUP BY differing_columns
            ORDER BY differing_columns
        """)
        crsr = self.execute(sql)
        while c := crsr.fetchone():
            n, cols = c
            click.echo(f"{n:>7}  {','.join(cols)}")

    def show_reason_dict_contents(self):
        sql = inspect.cleandoc("""
            SELECT reason_dict
            FROM reason_dict
            ORDER BY reason
        """)
        crsr = self.execute(sql)

        reasons = [x[0] for x in crsr.fetchall()]
        if not reasons:
            return
        max_name = max(len(x["reason"]) for x in reasons)
        for rd in reasons:
            click.echo(
                f" {rd['reason']:>{max_name}}:  {rd['description'] or dim('null')}"
            )

    def compare_tables(self):
        """
        Creates two tables from the two tables `mlwh` and `tolqc`. The tables
        contain the `data_id`, an MD5 hash of the entire row cast to VARCHAR, and
        the row itself as a DuckDB STRUCT.

        The join returns any rows where the `data_id` matches but the MD5 hash
        does not.

        ANTI JOINs to the `diff_store` table ignore any mismatches which have
        already been seen.
        """

        self.execute("SET temp_directory = '/tmp'")
        for name in ("mlwh", "tolqc"):
            self.build_h_table(name)
            self.cleanup_diff_store(name)

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
        crsr = self.execute(sql)

        while row := crsr.fetchone():
            yield Mismatch(*row)

    def build_h_table(self, name):
        """
        Create tempoary table with a hash of each row in table `name`
        """
        sql = inspect.cleandoc(f"""
            CREATE TEMPORARY TABLE {name}_h AS
              SELECT data_id
                , md5({name}::VARCHAR) AS hash
              FROM {name}
        """)  # noqa: S608
        self.execute(sql)

    def cleanup_diff_store(self, name):
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
        self.execute(sql)

    def create_diff_store(self):
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
        self.execute(sql)

    def create_reasons_tables(self):
        sql = inspect.cleandoc(
            """
            CREATE TABLE reason_dict(
                reason VARCHAR PRIMARY KEY
                , description VARCHAR
            )
            """
        )
        self.execute(sql)

        sql = inspect.cleandoc(
            """
            CREATE TABLE diff_reason(
                data_id VARCHAR
                , reason VARCHAR REFERENCES reason_dict (reason)
                , PRIMARY KEY (data_id, reason)
            )
            """
        )
        self.execute(sql)

    def create_or_update_macros_and_views(self):
        """Useful DuckDB macros and views"""

        for sql_def in (
            # Implements speciesops directory hash.
            # e.g. taxon_hash('9627') produces '2/e/9/7/6/a'
            """
            MACRO taxon_hash(str) AS
              md5(str)[:6].split('').array_to_string('/')
            """,
            # Converts species scientific name to hierarchy_name
            r"""
            MACRO species_hn(species) AS
                regexp_replace(species, '\W+', '_', 'g').trim('_')
            """,
            # Uses taxon_hash() and species_hn() to build path to lustre directory
            """
            MACRO species_lustre(taxon_str, species) AS
              CONCAT_WS('/'
                , '/lustre/scratch122/tol/data'
                , taxon_hash(taxon_str)
                , species_hn(species))
            """,
            # Creates a view of the tolqc table showing the species
            # directories. Uses the WOSPI ID for taxon_id = 32644
            # (unidentified) species.
            """
            VIEW species_dir AS
              SELECT DISTINCT scientific_name AS species_id
                , taxon_id
                , species_lustre(
                    IF(taxon_id = 32644, tol_specimen_id, taxon_id::VARCHAR)
                    , IF(taxon_id = 32644, tol_specimen_id, scientific_name)
                  ) AS directory
              FROM tolqc
              WHERE taxon_id IS NOT NULL
                AND scientific_name IS NOT NULL
              ORDER BY ALL
            """,
        ):
            self.execute("CREATE OR REPLACE " + sql_def)
