import mysql.connector
import json

from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def local_postgres_engine(**kwargs):
    engine = create_engine(
        "postgresql+psycopg2://sts-dev@127.0.0.1:5435/tolqc", **kwargs
    )
    return engine, sessionmaker(bind=engine, future=True)


def mlwh_db():
    return make_connection("mlwh")


def make_connection(db_alias):
    params_json = Path().home() / ".connection_params.json"
    params = json.loads(params_json.read_text())[db_alias]
    dbd = params.pop("dbd")
    if dbd == "mysql":
        return mysql.connector.connect(**params)
