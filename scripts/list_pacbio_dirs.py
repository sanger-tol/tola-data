import os
from pathlib import Path
from sys import stdout

import duckdb
from tola.ndjson import ndjson_row

conn = duckdb.connect(os.getenv("DIFF_MLWH_DUCKDB"))
conn.execute(
    """
    SELECT {
      'data.id': data_id,
      'specimen': tol_specimen_id,
      'directory': concat('/lustre/scratch123/tol/tolqc'
        , parse_dirpath(remote_path[7:])
        , '/reports'
      )
    }
    FROM tolqc
    WHERE starts_with(remote_path, 'irods:/seq/pacbio/')
      AND tol_specimen_id IS NOT NULL
    ORDER BY ALL
    """
)

for (row,) in conn.fetchall():
    file = Path(row["directory"])
    if file.is_dir():
        stdout.write(ndjson_row(row))
