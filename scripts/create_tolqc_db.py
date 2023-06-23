from main.model import Base
from sqlalchemy import create_engine, text
from . import db_connection


def main():
    engine, Session = db_connection.local_postgres_engine(echo=True)

    # SQLAlchemy cannot work out order of ..._status tables for drop_all
    # due to multiple foreign keys that link to their subject tables
    with Session() as ssn:
        for table in ('specimen_status', 'dataset_status', 'assembly_status'):
            ssn.execute(text(f"DROP TABLE {table} CASCADE"))
        ssn.commit()

    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


if __name__ == "__main__":
    main()
