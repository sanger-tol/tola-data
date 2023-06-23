from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def local_postgres_engine(**kwargs):
    engine = create_engine(
        "postgresql+psycopg2://sts-dev@127.0.0.1:5435/tolqc", **kwargs
    )
    return engine, sessionmaker(bind=engine, future=True)

