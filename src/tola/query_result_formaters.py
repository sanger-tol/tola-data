import json
import re
import sys


def output_tsv(session, query, file=sys.stdout):
    header = munge_header(column_names(query))
    print(tsv_row(header), file=file)
    for row in session.execute(query).all():
        print(tsv_row(row), file=file)


def output_json(session, query, file=sys.stdout):
    print("[", end="", file=file)
    row_itr = session.execute(query)
    if first_row := row_itr.fetchone():
        print(json.dumps(dict(first_row), separators=(",", ":")), end="", file=file)
        while row := row_itr.fetchone():
            print(",\n" + json.dumps(dict(row), separators=(",", ":")), end="", file=file)
    print("]", file=file)


def output_markdown(session, query, file=sys.stdout):
    pass


def tsv_row(row):
    strings = ("" if x is None else str(x) for x in row)
    return "\t".join(strings)


def column_names(query):
    return tuple(d["name"] for d in query.column_descriptions)


def munge_header(header):
    return tuple(munge_string(s) for s in header)


CASE_EXCEPTIONS = {
    "id": "ID",
    "mrna": "mRNA",
}


def munge_string(string):
    """
    Turns strings such as "col_name" into "Col Name"
    """
    words = []
    for w in re.split(r"_+", string.strip('_')):
        if w.islower():
            # Turn lowercase words into title case unless they should
            # have a know capitalisation in the exceptions dictionary.
            words.append(ce if (ce := CASE_EXCEPTIONS.get(w)) else w.title())
        else:
            # Preserve case if the word isn't all lowercase
            words.append(w)
    return " ".join(words)
