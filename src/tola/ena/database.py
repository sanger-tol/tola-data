from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import pyarrow

from tola.cache_db import CacheDB
from tola.ndjson import ndjson_row
from tola.tolqc_client import TolClient


class EnaCache(CacheDB):
    def __init__(
        self,
        path: Path | None,
        client: TolClient,
        write_flag=None,
    ):
        self.client = client
        self.needs_update = False
        super().__init__(
            path,
            True if write_flag is None else write_flag,
        )

    def create_db_tables(self):
        self.add_macros()

        tables = {x[0] for x in self.execute("SHOW TABLES").fetchall()}

        if "reason_dict" not in tables:
            self.create_reason_dict_table()
        if "error_reason" not in tables:
            self.create_reason_table()

        # If any of our data tables are missing, then an update is required
        if {"tolqc", "asm_data", "ena"} - tables:
            self.needs_update = True

        if "update_log" in tables:
            # Is the cached data stale?
            latest_row = self.execute(
                "SELECT MAX(updated_at) FROM update_log"
            ).fetchone()
            if latest_row is None:
                self.needs_update = True
            else:
                latest = latest_row[0]
                now = datetime.now(tz=latest.tzinfo)
                if now - latest > timedelta(hours=4):
                    self.needs_update = True
        else:
            self.execute("CREATE TABLE update_log(updated_at TIMESTAMPTZ)")
            self.needs_update = True

    def log_update_time(self):
        self.execute("INSERT INTO update_log(updated_at) VALUES (current_timestamp)")

    def add_macros(self):
        self.execute(r"""
          CREATE OR REPLACE MACRO extract_tolid (s) AS
            s.regexp_extract('^([^.]+)')
             .regexp_replace('\d+$', '');
        """)

    def create_reason_table(self):
        self.execute("""
          CREATE TABLE error_reason (
            genome_accession_id VARCHAR,
            run_accession VARCHAR,
            reason VARCHAR REFERENCES reason_dict (reason),
            PRIMARY KEY (genome_accession_id, run_accession)
          )
        """)

    def store_error_reasons(
        self,
        store_reason: str,
        input_objects: list[dict[str, str]],
    ):
        arrow_table = self.build_reasons_arrow_table(store_reason, input_objects)  # noqa: F841

        self.conn.execute("""
          INSERT INTO error_reason (
            genome_accession_id,
            run_accession,
            reason
          )
          SELECT
            genome_accession_id,
            run_accession,
            reason
          FROM arrow_table
          ON CONFLICT DO UPDATE SET reason = EXCLUDED.reason
          RETURNING *
        """)

        rows = self.conn.fetchall()
        return len(rows)

    def delete_error_reasons(
        self,
        delete_reason: str,
        input_objects: list[dict[str, str]],
    ) -> int:
        arrow_table = self.build_reasons_arrow_table(delete_reason, input_objects)  # noqa: F841
        conn = self.conn

        count = conn.execute("""
          SELECT
            count_star()
          FROM error_reason NATURAL JOIN arrow_table
        """).fetchone()

        conn.execute("""
          DELETE FROM error_reason
          USING arrow_table
          WHERE error_reason = arrow_table  -- Where row tuples match
        """)

        return 0 if count is None else count[0]

    def build_reasons_arrow_table(
        self,
        reason: str,
        input_objects: list[dict[str, str]],
    ):
        gen_accs = []
        run_accs = []
        for row in input_objects:
            gen_accs.append(row["genome_accession_id"])
            run_accs.append(row["run_accession"])
        return pyarrow.Table.from_pydict(
            {
                "genome_accession_id": gen_accs,
                "run_accession": run_accs,
                "reason": [reason] * len(gen_accs),
            }
        )

    def cache_tolqc_assemblies(self) -> None:
        sql = """
          CREATE OR REPLACE TABLE tolqc AS
          FROM
            read_json(
              ?,
              columns := {
                specimen_biosample: 'VARCHAR',
                specimen: 'VARCHAR',
                cobiont_of: 'VARCHAR',
                assembly_id: 'INT',
                assembly_bioproject: 'VARCHAR',
                genome_accession_id: 'VARCHAR',
                assembly_name: 'VARCHAR',
                description: 'VARCHAR',
                level: 'VARCHAR',
                status: 'VARCHAR',
                status_time: 'DATE',
              }
            );
        """
        url = self.client.report_url(
            "ena-assembly",
            params={"format": "NDJSON"},
        )
        self.execute(sql, [url])

    def cache_tolqc_assembly_datasets(self) -> None:
        sql = """
          CREATE OR REPLACE TABLE asm_data AS
          FROM
            read_json(
              ?,
              columns := {
                data_id:       'VARCHAR',
                run_accession: 'VARCHAR',
                specimen:      'VARCHAR',
                dataset_id:    'VARCHAR',
                dataset_name:  'VARCHAR',
                data_type:     'VARCHAR',
                assemblies:    'STRUCT(assembly_id INT, genome_accession_id VARCHAR)[]',
              }
            );
        """
        url = self.client.report_url(
            "ena-assembly-data",
            params={"format": "NDJSON"},
        )
        self.execute(sql, [url])

    def cache_ena_assemblies(self, accession="PRJEB43745") -> None:
        """
        Cache assembly information from the ENA `filereport` endpoint
        """

        ena_fields = {
            "specimen_biosample": "sample_accession",
            "genome_accession_id": "assembly_set_accession",
            "assembly_bioproject": "study_accession",
            "assembly_name": "assembly_name",
            "description": "assembly_title",
            "level": "assembly_level",
            "status": "status",
            "status_time": "last_updated",
            "run_accession_list": "run_accession",
        }

        # Build ENA filereport query URL
        params = "&".join(
            f"{k}={v}"
            for k, v in {
                "result": "assembly",
                "format": "CSV",
                "accession": accession,
                "fields": ",".join(ena_fields.values()),
            }.items()
        )
        filereport_url = f"https://www.ebi.ac.uk/ena/portal/api/filereport?{params}"
        self.log.debug(f"{filereport_url = }")

        # Use DuckDB to split run_accession_list string into a list on import
        ena_fields["run_accession_list"] = "string_split(run_accession, ';')"

        column_specs = ",\n          ".join(
            f"{col} AS {alias}" for alias, col in ena_fields.items()
        )

        # Fetch ENA records for this accession
        sql = f"""
          CREATE OR REPLACE TABLE ena AS
            SELECT
              {column_specs}
            FROM
              read_csv(?)
        """  # noqa: S608
        try:
            self.execute(sql, [filereport_url])
        except duckdb.BinderException:
            # Empty result for accession
            return

    def load_ena_assemblies(self, loaded: list[dict[str, str]]) -> None:
        # Get new ENA assembly records by joining to the tolqc table.

        # Filter results from ENA by those which have a matching BioSample
        # accession in the ToLQC specimen table, then look for any GCA accessions
        # which aren't in the assembly table.
        self.execute("""
          WITH
            sbs AS (
              SELECT DISTINCT
                specimen_biosample,
                tolqc.specimen
              FROM
                ena
                JOIN tolqc USING (specimen_biosample)
            )
          SELECT
            tolqc.assembly_id,
            sbs.specimen,
            ena.*
          FROM
            ena
            JOIN sbs USING (specimen_biosample)
            LEFT JOIN tolqc USING (specimen_biosample, assembly_name)
          WHERE
            tolqc.genome_accession_id IS NULL
          ORDER BY
            sbs.specimen,
            assembly_name
        """)

        # Mapping of ENA status names to our `assembly_status_id`
        status_names = {"public": "ENA Public"}

        client = self.client
        ads = client.ads
        cdo = client.build_cdo
        for arrow_batch in self.conn.fetch_record_batch(client.page_size):
            batch = arrow_batch.to_pydict()
            accession_cdo = []
            assembly_cdo = []
            statuses = {}
            for i in range(arrow_batch.num_rows):
                # GenBank assembly accession
                acc_sv = batch["genome_accession_id"][i]
                accession_cdo.append(
                    cdo(
                        "accession",
                        acc_sv,
                        {"accession_type_id": "GenBank Genome Assembly"},
                    )
                )

                # Assembly BioProject accession
                bio_acc = batch["assembly_bioproject"][i]
                accession_cdo.append(
                    cdo(
                        "accession",
                        bio_acc,
                        {"accession_type_id": "BioProject - Species Assembly"},
                    )
                )

                # Assembly table entry
                assembly_cdo.append(
                    cdo(
                        "assembly",
                        batch["assembly_id"][i],
                        {
                            "genome_accession_id": acc_sv,
                            "bioproject_accession_id": bio_acc,
                            "specimen_id": batch["specimen"][i],
                            "name": batch["assembly_name"][i],
                            "description": batch["description"][i],
                            "level": batch["level"][i],
                            "category_id": "release",
                        },
                    )
                )
                status = batch["status"][i]

                statuses[acc_sv] = {
                    "status_type.id": status_names.get(status, status),
                    "status_time": batch["status_time"][i],
                }

                loaded.append(
                    {
                        "specimen.id": batch["specimen"][i],
                        "assembly_name": batch["assembly_name"][i],
                        "bioproject_accession.id": bio_acc,
                        "genome_accession.id": acc_sv,
                    }
                )

            # Use `upsert()` to insert the accessions in case any are already
            # loaded.
            ads.upsert("accession", accession_cdo)

            assemblies = ads.upsert("assembly", assembly_cdo)
            status_json = []
            for asm in assemblies:
                acc_sv = asm.genome_accession.id
                status = statuses[acc_sv]
                status_json.append(ndjson_row({"assembly.id": asm.id, **status}))
            client.ndjson_post("loader/status/assembly", status_json)

    def load_ena_assembly_datasets(self) -> list[dict[str, str]]:
        new_links = []

        self.execute("""
          WITH
            ena_run AS (
              SELECT
                genome_accession_id,
                unnest(run_accession_list) AS run_accession
              FROM
                ena
            ),
            ena_run_sets AS (
              SELECT
                genome_accession_id,
                data_type,
                list_sort(array_agg(run_accession)) AS run_accession_list
              FROM
                ena_run
                JOIN asm_data USING (run_accession)
              GROUP BY
                genome_accession_id,
                data_type
            ),
            dataset_accn AS (
              SELECT
                dataset_id,
                dataset_name,
                list_sort(array_agg(run_accession)) AS run_accession_list
              FROM
                asm_data
              GROUP BY
                dataset_id,
                dataset_name
            ),
            dataset_asm AS (
              SELECT DISTINCT
                dataset_id,
                unnest(assemblies, recursive := true)
              FROM
                asm_data
              WHERE
                assemblies IS NOT NULL
            )
          SELECT
            assembly_id,
            assembly_name,
            dataset_name,
            dataset_id
          FROM
            ena_run_sets
            JOIN dataset_accn USING (run_accession_list)
            JOIN tolqc USING (genome_accession_id)
            ANTI JOIN dataset_asm USING (genome_accession_id, dataset_id)
          ORDER BY
            assembly_name,
            dataset_name,
            dataset_id
        """)

        client = self.client
        ads = client.ads
        cdo = client.build_cdo
        for arrow_batch in self.conn.fetch_record_batch(client.page_size):
            batch = arrow_batch.to_pydict()
            assembly_dataset_cdo = []
            for i in range(arrow_batch.num_rows):
                assembly_dataset_cdo.append(
                    cdo(
                        "assembly_dataset",
                        None,
                        {
                            "assembly_id": batch["assembly_id"][i],
                            "dataset_id": batch["dataset_id"][i],
                        },
                    )
                )
                new_links.append(
                    {
                        "assembly.name": batch["assembly_name"][i],
                        "dataset.name": batch["dataset_name"][i],
                        "dataset.id": batch["dataset_id"][i],
                    }
                )
            ads.upsert("assembly_dataset", assembly_dataset_cdo)

        return new_links
