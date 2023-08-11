import click
import os
import mysql.connector
import psycopg2
import json
import urllib.parse

from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from psycopg2.extras import DictCursor


tolqc_db = click.option(
    '--tolqc-db',
    envvar="TOLQC_DB",
    default="tolqc-staging",
    show_default=True,
    help="Alias of database in ~/.connection_params.json to connect to",
)


def tola_db_engine(db_alias="tolqc-staging", **kwargs):
    engine = create_engine(get_connection_url(db_alias), **kwargs)
    return engine, sessionmaker(bind=engine, future=True)


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
        lib_spec = "mysql+mysqlconnectory"
    elif dbd == "Pg":
        lib_spec = "postgresql+psycopg2"
    else:
        raise f"Unsupported database type '{dbd}'"

    # dialect+driver://username:password@host:port/database
    return (
        f"{lib_spec}://{params['user']}:{enc_pass}"
        f"@{params['host']}:{params['port']}/{params['database']}"
    )


def get_connection_params_entry(db_alias):
    params_name = ".connection_params.json"
    params_file = Path().home() / params_name

    # Check permissions are 0600
    mode = params_file.stat().st_mode & 0o777
    if mode != 0o600:
        msg = f"~/{params_name} must be mode 0600 but is mode 0{mode:o}"
        raise Exception(msg)

    if db_params := json.loads(params_file.read_text()).get(db_alias):
        return db_params
    else:
        msg = f"Database alias '{db_alias}' not found in ~/{params_name} file"
        raise Exception(msg)
