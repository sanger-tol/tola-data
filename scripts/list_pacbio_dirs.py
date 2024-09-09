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
    ORDER BY ALL;
    """
)

for (row,) in conn.fetchall():
    file = Path(row["directory"])
    if file.is_dir():
        stdout.write(ndjson_row(row))
