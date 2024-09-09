import os
from pathlib import Path
from sys import stdout

import duckdb
from tola.ndjson import ndjson_row

conn = duckdb.connect(os.getenv("DIFF_MLWH_DUCKDB"))
conn.execute(
    """
    SELECT DISTINCT {
      'pacbio_run_metrics.id': run_id,
      'directory': CONCAT(
          '/lustre/scratch123/tol/tolqc'
        , parse_dirpath(remote_path[7:])
        , '/reports'
      )
    }
    FROM tolqc
    WHERE platform_type = 'PacBio'
      AND starts_with(remote_path, 'irods:')
      AND (
        -- pabcio_run_metrics fields:
           movie_minutes IS NOT NULL
        OR binding_kit IS NOT NULL
        OR sequencing_kit IS NOT NULL
        OR sequencing_kit_lot_number IS NOT NULL
        OR cell_lot_number IS NOT NULL
        OR include_kinetics IS NOT NULL
        OR loading_conc IS NOT NULL
        OR control_num_reads IS NOT NULL
        OR control_read_length_mean IS NOT NULL
        OR control_concordance_mean IS NOT NULL
        OR control_concordance_mode IS NOT NULL
        OR local_base_rate IS NOT NULL
        OR polymerase_read_bases IS NOT NULL
        OR polymerase_num_reads IS NOT NULL
        OR polymerase_read_length_mean IS NOT NULL
        OR polymerase_read_length_n50 IS NOT NULL
        OR insert_length_mean IS NOT NULL
        OR insert_length_n50 IS NOT NULL
        OR unique_molecular_bases IS NOT NULL
        OR productive_zmws_num IS NOT NULL
        OR p0_num IS NOT NULL
        OR p1_num IS NOT NULL
        OR p2_num IS NOT NULL
        OR adapter_dimer_percent IS NOT NULL
        OR short_insert_percent IS NOT NULL
        OR hifi_read_bases IS NOT NULL
        OR hifi_num_reads IS NOT NULL
        OR hifi_read_length_mean IS NOT NULL
        OR hifi_read_quality_median IS NOT NULL
        OR hifi_number_passes_mean IS NOT NULL
        OR hifi_low_quality_read_bases IS NOT NULL
        OR hifi_low_quality_num_reads IS NOT NULL
        OR hifi_low_quality_read_length_mean IS NOT NULL
        OR hifi_low_quality_read_quality_median IS NOT NULL
        OR hifi_barcoded_reads IS NOT NULL
        OR hifi_bases_in_barcoded_reads IS NOT NULL
      )
    ORDER BY ALL;
    """
)

for (row,) in conn.fetchall():
    file = Path(row["directory"])
    if file.is_dir():
        stdout.write(ndjson_row(row))
