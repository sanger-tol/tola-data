#!/usr/bin/env python3

import logging
from subprocess import PIPE, Popen

from tolqc.reports import mlwh_data_report_query_select

log = logging.getLogger(__name__)


def main():
    """
    Fetches table and column definitions used by `diff-mlwh`.

    Kept in a separate script to avoid dependency on ToLQC server side code.
    """

    query = mlwh_data_report_query_select()

    with Popen(  # noqa: S603
        ["ruff", "format", "--silent", "--stdin-filename", "-"],
        stdin=PIPE,  # noqa: S607
    ) as ruff_format:
        table_map = build_table_map(query)
        ruff_format.stdin.write(f"\n{table_map = }\n".encode())

        col_defs = column_definitions(query)
        ruff_format.stdin.write(f"\n{col_defs = }\n".encode())


def build_table_map(query):
    table_map = {
        "file": {"data_id": "data.id"},
        "pacbio_run_metrics": {"run_id": "pacbio_run_metrics.id"},
        # "platform": {"run_id": "run.id"},
    }

    for name, tbl, col in name_table_column(query):
        # if col.name == "library_type_id":
        #     click.echo(f"{col.name = } {col.foreign_keys = }", err=True)
        out_name = f"{tbl}.id" if col.primary_key else col.name
        table_map.setdefault(tbl, {})[name] = out_name

    return table_map


def column_definitions(query):
    col_defs = {}

    debug_str = "Column types from mlwh-data query:\n"
    for name, _, col in name_table_column(query):
        type_ = "TIMESTAMPTZ" if (s := str(col.type)) == "DATETIME" else s
        debug_str += f"  {name} = {type_}\n"
        col_defs[name] = type_
    log.debug(debug_str)

    return col_defs


def name_table_column(query):
    """
    Returns a list of tuples (name, Table, Column) for each column of the
    SQLAlchemy `query` argument.
    """

    cols = []
    for desc in query.column_descriptions:
        # {
        #     "name": "sample_name",
        #     "type": String(),
        #     "aliased": False,
        #     "expr": "<sqlalchemy.sql.elements.Label at 0x10bf3fe80; sample_name>",
        #     "entity": "<class 'tolqc.sample_data_models.Sample'>",
        # }
        name = desc["name"]
        if name == "supplier_name":
            continue
        tbl = desc["entity"].__tablename__
        expr = desc["expr"]
        if hasattr(expr, "base_columns"):
            (col,) = expr.base_columns
        else:
            (col,) = expr.columns.values()
        cols.append((name, tbl, col))

    return cols


if __name__ == "__main__":
    main()
