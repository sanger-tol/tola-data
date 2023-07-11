import pathlib
import sys

from tola import db_connection
from main.model import Base


def main(dict_tsv_file_names):
    engine, Session = db_connection.tola_db_engine(echo=True)

    table_class = table_name_to_class()

    for tn in table_class:
        print(f"table = '{tn}'")

    with Session() as ssn:
        for dict_tsv_file in (pathlib.Path(x) for x in dict_tsv_file_names):
            table_name = dict_tsv_file.stem
            cls = table_class[table_name]
            for obj in read_tsv(cls, dict_tsv_file):
                ssn.merge(obj)
        ssn.commit()


def table_name_to_class():
    return {
        cls.__tablename__: cls
        for cls in (mppr.class_ for mppr in Base.registry.mappers)
    }


def read_tsv(cls, path):
    with path.open() as fh:
        header_line = split_tsv_line(fh.readline())
        for line in fh:
            obj = cls()
            cols = split_tsv_line(line)
            for attr, val in zip(header_line, cols):
                setattr(obj, attr, val)
            yield obj


def split_tsv_line(line):
    return list(None if x == r"\N" else x for x in line.rstrip("\r\n").split("\t"))


if __name__ == "__main__":
    main(sys.argv[1:])
