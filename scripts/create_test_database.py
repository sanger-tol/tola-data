"""
Create a non-ephermeral copy of the test data used for system tests in ToLQC.

Recreate test data if required (under a venv with ToLQC API):

  DB_URI=postgresql://tolqc-dev@127.0.0.1:5435/tolqc \
    python3 scripts/fetch_test_data.py > \
    tolqc-api/app/test/system/data_objects.py

On the test server run:

  CREATE DATABASE tolqc_test;

then run this script with a new `DB_URI`:

  DB_URI=postgresql://tolqc-dev@127.0.0.1:5435/tolqc_test \
    python3 scripts/create_test_database.py

"""

import logging
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, configure_mappers
from test.system.data_objects import test_data
from tolqc.schema.base import Base


def test_create_db():
    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
    configure_mappers()
    db_uri = os.getenv("DB_URI")
    engine = create_engine(db_uri)
    connection = engine.connect()
    Base.metadata.create_all(connection)
    connection.commit()

    with Session(engine) as ssn:
        for obj in test_data("TEST-TOKEN"):
            ssn.merge(obj)
        ssn.commit()



if __name__ == "__main__":
    test_create_db()
