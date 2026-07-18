import logging
import os
from abc import abstractmethod
from inspect import cleandoc
from pathlib import Path
from urllib.request import getproxies

import click
import duckdb

from tola.pretty import dim


class CacheDB:
    """
    Base class for DuckDB databases used as caches
    """

    def __init__(
        self,
        path: Path | None = None,
        write_flag: bool = True,
    ):
        self.log = logging.getLogger(self.__class__.__name__)
        if path:
            if not path.exists():
                write_flag = True
        else:
            write_flag = True

        self.conn = duckdb.connect(
            path or ":memory:",
            read_only=not write_flag,
        )
        self.setup_ca_cert_file()
        self.setup_http_proxy()
        if write_flag:
            self.create_db_tables()

    def setup_ca_cert_file(self):
        """
        Sets a CA certificates file for DuckDB's libcurl HTTP client.
        """
        if cert_file := os.environ.get("REQUESTS_CA_BUNDLE"):
            self.execute("SET ca_cert_file = ?", [cert_file])
            self.execute("SET enable_server_cert_verification = true")

    def setup_http_proxy(self):
        """
        Sets HTTP proxy URL (which may have been set by `TolClient`) for
        DuckDB's libcurl HTTP client.
        """
        pxs = getproxies()
        if proxy_url := pxs.get("https", pxs.get("http")):
            self.execute("SET http_proxy = ?", [proxy_url])

    def execute(self, sql: str, params=None) -> duckdb.DuckDBPyConnection:
        sql.rstrip("; \n")
        self.log.debug(
            f"{cleandoc(sql)};\n"
            + "".join(f"  p{i + 1}: {p!r}\n" for i, p in enumerate(params or ()))
        )
        return self.conn.execute(sql, params)

    @abstractmethod
    def create_db_tables(self):
        """
        Called by `__init__` to create any missing database tables.
        """

    def create_reason_dict_table(self):
        self.execute("""
          CREATE TABLE reason_dict (
            reason VARCHAR PRIMARY KEY,
            description VARCHAR
          )
        """)

    def load_reason_dict_entry(self, reason_dict_row: tuple[str, str]):
        sql = """
          INSERT OR REPLACE INTO reason_dict(reason, description)
          VALUES (?,?)
        """
        self.execute(sql, reason_dict_row)

    def load_reason_dict_ndjson(self, reason_dict_ndjson):
        file = "/dev/stdin" if reason_dict_ndjson == "-" else reason_dict_ndjson
        sql = """
          INSERT OR REPLACE INTO
            reason_dict (reason, description)
          FROM
            read_json(?, columns = {reason: 'VARCHAR', description: 'VARCHAR'})
        """
        self.execute(sql, (file,))

    def show_reason_dict_contents(self):
        sql = """
          SELECT reason_dict
          FROM reason_dict
          ORDER BY reason
        """
        crsr = self.execute(sql)

        reasons = [x[0] for x in crsr.fetchall()]
        if not reasons:
            return
        max_name = max(len(x["reason"]) for x in reasons)
        for rd in reasons:
            click.echo(
                f" {rd['reason']:>{max_name}}:  {rd['description'] or dim('null')}"
            )
