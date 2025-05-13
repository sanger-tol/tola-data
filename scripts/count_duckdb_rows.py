#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["duckdb"]
# ///

import sys
from pathlib import Path

import duckdb


def cli():
    if len(sys.argv) != 2:
        sys.exit(f"Usage: {Path(sys.argv[0]).name} <DUCKDB_FILE>")
    conn = duckdb.connect(sys.argv[1], read_only=True)
    for (table,) in conn.execute("SHOW TABLES").fetchall():
        (n,) = conn.execute(f"SELECT count_star() FROM {table}").fetchone()  # noqa: S608
        print(f"{n:>12,}  {table}")


if __name__ == "__main__":
    cli()
