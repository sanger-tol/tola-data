#!/usr/bin/env python3

import os
import re
import subprocess
import sys
from pathlib import Path

from tol.api_client import ApiDataSource, ApiObject
from tol.core.datasource_error import DataSourceError


def main(acc_files):
    accession_rows = []
    for file in acc_files:
        accession_data_from_file(file, accession_rows)

    ads = ApiDataSource(
        {
            "url": os.getenv("TOLQC_URL") + "/api/v1",
            "key": os.getenv("TOLQC_API_KEY"),
        },
    )

    try:
        ads.create(accession_rows)
    except DataSourceError as err:
        err_string, err_trace, err_code = err.args
        if re.match(r"^\<\!doctype html\>", err_trace):
            err_file_html = "error.html"
            print(f"{err_string}: {err_code}", file=sys.stderr)
            Path(err_file_html).write_text(err_trace)
            subprocess.run(["open", err_file_html])
        else:
            print(err)


def accession_data_from_file(file, rows=[]):
    """
    Parse file of accessions with tab-separated lines such as:

      /seq/20101/20101_1#1.cram  ERR1725638  ERX1795601  ERP014164  ERS1078382  2016-11-09
      /seq/20101/20101_2#1.cram  ERR1725639  ERX1795602  ERP014164  ERS1078382  2016-11-09
    """
    with Path(file).open() as acc_file:
        for line in acc_file:
            if not re.search(r"\w", line):
                continue  # Skip blank lines

            (
                file,
                accession,
                secondary,
                project,
                biosample,
                submission_date,
            ) = line.rstrip().split("\t")

            acc = ApiObject(
                "accessions",
                None,
                attributes={
                    "name": accession,
                    "secondary": secondary,
                    "date_submitted": f"{submission_date}T00:00:00Z",
                },
            )
            rows.append(acc)
            # print(f"<{submission_date}>", file=sys.stderr)

    return rows


if __name__ == "__main__":
    main(sys.argv[1:])
