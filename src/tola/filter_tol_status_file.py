#!/usr/bin/env python3

import datetime
import io
import re
import requests
import sys
from pathlib import Path


def main(tsv_files):
    if tsv_files:
        for file in tsv_files:
            fixup_status_tsv_file(file)
    else:
        input_io = fetch_current_sheet()
        fixed = today_status_path()
        fixup_status_data(input_io, fixed)


def fetch_current_sheet():
    document_id = "1RKubj10g13INd4W7alHkwcSVX_0CRvNq0-SRe21m-GM"
    url = (
        f"https://docs.google.com/spreadsheets/d/{document_id}/export?exportFormat=tsv"
    )
    r = requests.get(url)
    if r.status_code == requests.codes.ok:
        # Encoding was 'ISO-8859-1'.  Setting to apparent sets 'utf-8',
        # correctly encoding bullet characters in spreadsheet.
        r.encoding = r.apparent_encoding
        return io.StringIO(r.text)
    else:
        r.raise_for_status()


def fixup_status_tsv_file(file):
    input = Path(file)
    fixed = construct_date_stamped_path(input)

    input_io = input.open()
    fixup_status_data(input_io, fixed)


def fixup_status_data(input_io, fixed):
    fixed_io = fixed.open(mode="w", encoding="utf8")
    head = input_io.readline().rstrip().split("\t")
    head = cleanup_header(head)
    fixed_io.write("\t".join(head) + "\n")

    col_count = len(head)
    row_n = 1
    for line in input_io:
        row_n += 1

        # Skip blank lines
        if not re.search(r"\w", line):
            continue

        row = line.rstrip("\r\n").split("\t")
        row[0] = str(row_n)

        if len(row) != col_count:
            raise ValueError(
                f"Row {row_n} has {len(row)} columns,"
                f" but header has {col_count} columns"
            )

        fixed_io.write("\t".join(row) + "\n")

    print(f"Wrote to '{fixed}'", file=sys.stderr)
    return fixed


def cleanup_header(dirty):
    clean = [make_identifier(x) for x in dirty]
    if not clean[0]:
        clean[0] = "row"
    if clean[5] == "sample":
        clean[5] = "specimen"
    return clean


def make_identifier(txt):
    idtfyr = re.sub(r"\W+", "_", re.sub(r"&", "_and_", txt))
    return idtfyr.strip("_")


def construct_date_stamped_path(path):
    mod_time = datetime.datetime.fromtimestamp(path.stat().st_mtime)
    fixed = Path(re.sub(r"[-\s]+", "_", path.name))
    return fixed.with_stem(fixed.stem + "_" + mod_time.date().isoformat())


def today_status_path():
    today = datetime.date.today().isoformat()
    return Path(f"Tree_of_Life_assembly_informatics_Status_{today}.tsv")


if __name__ == "__main__":
    main(sys.argv[1:])
