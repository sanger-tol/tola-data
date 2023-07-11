import os
import mysql.connector
import psycopg2
import json
import urllib.parse

from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from psycopg2.extras import DictCursor


def tola_db_engine(db_alias=os.getenv('TOLA_DB', 'tol-local'), **kwargs):
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
    params_json = Path().home() / ".connection_params.json"
    return json.loads(params_json.read_text())[db_alias]
