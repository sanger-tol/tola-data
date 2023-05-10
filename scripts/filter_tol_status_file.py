#!/usr/bin/env python3

import datetime
import re
import sys
from pathlib import Path


def main(tsv_files):
    for file in tsv_files:
        fixup_status_tsv_file(file)


def fixup_status_tsv_file(file):
    input = Path(file)
    fixed = construct_date_stamped_path(input)

    input_io = input.open()
    fixed_io = fixed.open(mode="w")
    head = input_io.readline().rstrip().split("\t")
    head[0] = "row"
    fixed_io.write("\t".join(head) + "\n")

    col_count = len(head)
    row_n = 1
    for line in input_io:
        row_n += 1

        # Skip blanks lines
        if not re.search(r"\w", line):
            continue

        row = line.rstrip("\r\n").split("\t")
        row[0] = str(row_n)

        row_cols = len(row)
        if row_cols != col_count:
            raise ValueError(
                f"Row {row_n} has {row_cols} columns,"
                f" but header has {col_count} columns"
            )

        # Pad row to the same length as the header to
        # silence warnings when importing into SQLite
        row.extend([""] * (col_count - row_cols))

        fixed_io.write("\t".join(row) + "\n")

    print(f"Wrote to '{fixed}'", file=sys.stderr)
    return fixed


def construct_date_stamped_path(path):
    mod_time = datetime.datetime.fromtimestamp(path.stat().st_mtime)
    fixed = Path(re.sub(r"[-\s]+", "_", path.name))
    return fixed.with_stem(fixed.stem + "_" + mod_time.date().isoformat())


if __name__ == "__main__":
    main(sys.argv[1:])
