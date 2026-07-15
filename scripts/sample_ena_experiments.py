import sys
from random import randint

import duckdb
import requests

from tola.ena.ena_expt_xml_parser import EnaExptXmlParser
from tola.ndjson import ndjson_row


def main():
    conn = duckdb.connect()
    conn.execute("""
      ATTACH 'postgresql://tolqc-dev@127.0.0.1:5435/tolqc'
      AS tqc (TYPE postgres)
    """)

    conn.execute("""
      SELECT
        library_type_id, experiment_accession_id
      FROM
        tqc.data_submission
        JOIN tqc.data USING (data_id)
        JOIN tqc.library USING (library_id)
      ORDER BY ALL
    """)

    # Use reservoir sampling to get up to this many samples for each lib type
    rsrv_size = 20
    samples = {}
    for lib_type, acc in conn.fetchall():
        n_smpl = samples.setdefault(lib_type, [0, []])
        n, smpl = n_smpl
        n += 1
        if n <= rsrv_size:
            smpl.append(acc)
        elif (i := randint(0, n - 1)) < rsrv_size:  # noqa: S311
            smpl[i] = acc
        n_smpl[0] = n

    for lib_type, n_smpl in samples.items():
        n, smpl = n_smpl
        for acc in sorted(smpl):
            print(f"{lib_type}\t{n}\t{acc}", file=sys.stderr)
            url = f"https://www.ebi.ac.uk/ena/browser/api/xml/{acc}"
            rspns = requests.get(url, timeout=10)
            if rspns.status_code != 200:
                sys.exit(f"{rspns.status_code}: {url}")
            xml = rspns.content
            for doc in EnaExptXmlParser().parse_string(xml):
                sys.stdout.write(ndjson_row({"library_type": lib_type, **doc}))



if __name__ == "__main__":
    main()
