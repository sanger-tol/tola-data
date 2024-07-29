#!/usr/bin/env python3

import inspect
from subprocess import PIPE, Popen


def main():
    lucidchart_sql = inspect.cleandoc(
        """
        SELECT 'postgresql' AS dbms
          , t.table_catalog
          , t.table_schema
          , t.table_name
          , c.column_name
          , c.ordinal_position
          , c.data_type
          , c.character_maximum_length
          , n.constraint_type
          , k2.table_schema
          , k2.table_name
          , k2.column_name
        FROM information_schema.tables t
        NATURAL LEFT JOIN information_schema.columns c
        LEFT JOIN(
            information_schema.key_column_usage k
            NATURAL JOIN information_schema.table_constraints n
            NATURAL LEFT JOIN information_schema.referential_constraints r
        )
          ON c.table_catalog = k.table_catalog
          AND c.table_schema = k.table_schema
          AND c.table_name = k.table_name
          AND c.column_name = k.column_name
        LEFT JOIN information_schema.key_column_usage k2
          ON k.position_in_unique_constraint = k2.ordinal_position
          AND r.unique_constraint_catalog = k2.constraint_catalog
          AND r.unique_constraint_schema = k2.constraint_schema
          AND r.unique_constraint_name = k2.constraint_name
        WHERE t.TABLE_TYPE = 'BASE TABLE'
          AND t.table_schema NOT IN('information_schema', 'pg_catalog')
        """
    )

    # Rename data types for more compact text in ERD
    data_type_rename = {
        "character varying": "varchar",
        "double precision": "float",
        "timestamp with time zone": "timestamp",
        "timestamp without time zone": "timestamp",
    }

    # Not interested in audit log or folder columns in diagram
    skip_column = {
        "created_by",
        "created_at",
        "modified_by",
        "modified_at",
        "history",
        "folder_ulid",
    }

    # LucidChart has the suggested "SET enable_nestloop=off" command for psql.
    # It makes the query around 15 times faster. From the PostgreSQL documentation:
    #
    # enable_nestloop (boolean)
    #
    #   Enables or disables the query planner's use of nested-loop join plans.
    #   It is impossible to suppress nested-loop joins entirely, but turning
    #   this variable off discourages the planner from using one if there are
    #   other methods available. The default is on.
    psql_cmd = [
        "psql",
        "--host=127.0.0.1",
        "--port=5435",
        "--user=tolqc-dev",
        "tolqc",
        "-c",
        f"SET enable_nestloop=off; COPY ({lucidchart_sql}) TO STDOUT WITH NULL AS ''",
    ]

    with Popen(psql_cmd, stdout=PIPE, text=True) as psql_pipe:  # noqa: S603
        for row_str in psql_pipe.stdout:
            row = row_str.rstrip("\r\n").split("\t")

            # Skip header output from psql
            if len(row) != 12:
                continue

            # Skip single letter tables "A".."H"
            if len(row[3]) == 1:
                continue

            # Skip audit log columns
            if row[4] in skip_column:
                continue

            data_type = row[6]
            row[6] = data_type_rename.get(data_type, data_type)
            print("\t".join(row))


if __name__ == "__main__":
    main()
