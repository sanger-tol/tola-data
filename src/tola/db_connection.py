import json
import urllib.parse
from pathlib import Path

import mysql.connector
import psycopg2
from psycopg2.extras import DictCursor


class ConnectionParamsError(Exception):
    """Error in the ~/.connection_params.json config file"""


def mlwh_db():
    return make_connection("mlwh")


def sts_db():
    return make_connection("sts")


def make_connection(db_alias):
    params = get_connection_params_entry(db_alias)
    dbd = params.pop("dbd")
    if dbd == "mysql":
        return mysql.connector.connect(**params)
    elif dbd == "Pg":
        return psycopg2.connect(cursor_factory=DictCursor, **params)
    else:
        raise f"Unknown database type '{dbd}'"


def get_connection_url(db_alias):
    params = get_connection_params_entry(db_alias)
    enc_pass = urllib.parse.quote_plus(params["password"])
    dbd = params["dbd"]
    if dbd == "mysql":
        lib_spec = "mysql+mysqlconnector"
    elif dbd == "Pg":
        lib_spec = "postgresql+psycopg2"
    else:
        msg = f"Unsupported database type '{dbd}'"
        raise ValueError(msg)

    # dialect+driver://username:password@host:port/database
    return (
        f"{lib_spec}://{params['user']}:{enc_pass}"
        f"@{params['host']}:{params['port']}/{params['database']}"
    )


def get_connection_params_entry(alias, no_params_file_ok=False):
    params_name = ".connection_params.json"
    params_file = Path().home() / params_name

    if no_params_file_ok and not params_file.exists():
        return None

    try:
        mode = params_file.stat().st_mode & 0o777
    except FileNotFoundError:
        msg = f"Missing ~/{params_name} file"
        raise ConnectionParamsError(msg) from None

    # Check permissions are 0600
    if mode != 0o600:
        msg = f"~/{params_name} must be mode 0600 but is mode 0{mode:o}"
        raise ConnectionParamsError(msg)

    try:
        if db_params := json.loads(params_file.read_text()).get(alias):
            return db_params
        else:
            msg = f"Alias '{alias}' not found in ~/{params_name} file"
            raise ConnectionParamsError(msg)
    except json.decoder.JSONDecodeError as jde:
        detail = "\n".join(jde.args)
        msg = f"Syntax error in ~/{params_name} - {detail}"
        raise ConnectionParamsError(msg) from None
