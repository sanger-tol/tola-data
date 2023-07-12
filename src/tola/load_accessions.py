#!/usr/bin/env python3

import os
import re
import subprocess
import sys
import tola.marshals

from main.model import Accession
from pathlib import Path
from tol.api_client import ApiDataSource, ApiObject
from tol.core.datasource_error import DataSourceError


def main():
    mrshl, acc_files = tola.marshals.marshal_from_command_line(
        "Import accession data from the tol_subtrack.fofn (or similar) file"
    )
    for file in acc_files:
        for row in accession_data_from_file(file):
            mrshl.fetch_or_create(
                Accession,
                {
                    "accession_id": row["run_acc"],
                    "accession_type_id": "ENA Run",
                    "secondary": row["expt_acc"],
                    "date_submitted": f"{row['run_submission_date']}T00:00:00Z",
                },
            )

            mrshl.fetch_or_create(
                Accession,
                {
                    "accession_id": row["project_acc"],
                    "accession_type_id": "Bio Project",
                },
            )

            mrshl.fetch_or_create(
                Accession,
                {
                    "accession_id": row["biosample_acc"],
                    "accession_type_id": "Bio Sample",
                },
            )
    mrshl.commit()


def accession_data_from_file(file):
    """Parse file of accessions with tab-separated lines such as:

    /seq/20101/20101_1#1.cram  ERR1725638  ERX1795601  ERP014164  ERS1078382  2016-11-09
    /seq/20101/20101_2#1.cram  ERR1725639  ERX1795602  ERP014164  ERS1078382  2016-11-09
    """
    column_names = (
        "file",
        "run_acc",
        "expt_acc",
        "project_acc",
        "biosample_acc",
        "run_submission_date",
    )
    for line in Path(file).open():
        # Skip blank lines
        if not re.search(r"\w", line):
            continue

        data = line.rstrip("\r\n").split("\t")
        yield {col: val for col, val in zip(column_names, data)}


if __name__ == "__main__":
    main()
