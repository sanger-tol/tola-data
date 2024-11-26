import csv
import json
import pathlib
import re

import click

FILE_TYPE = click.Path(
    dir_okay=False,
    exists=True,
    readable=True,
    path_type=pathlib.Path,
)
DEFAULT_FILENAMES = "data/pacbio_tolqc_data.json", "data/pacbio_run_report.json"


@click.command(
    help="Compare PacBio data.json from TolQC website to run report from database"
)
@click.option(
    "--compare",
    type=(FILE_TYPE, FILE_TYPE),
    default=DEFAULT_FILENAMES,
    help=(
        "data.json file from TolQC website"
        " and JSON formatted report from pacbio-run-report script"
    ),
    show_default=True,
)
@click.option(
    "--diff-fields",
    is_flag=True,
    default=False,
    show_default=True,
    help="Only show fields in each record which differ",
)
@click.option(
    "--key-counts",
    type=FILE_TYPE,
    is_flag=False,
    show_default=True,
    help="Show occupation count of keys in file",
)
def cli(compare, diff_fields, key_counts):
    if key_counts:
        show_key_count(key_counts)
    else:
        main(*compare, diff_fields)


def main(json_input, rprt_input, diff_fields):
    headers = (
        "idx",
        "source",
        "movie_name",
        "tag_index",
        "project",
        "specimen",
        "sanger_id",
        "pipeline",
        "platform",
        "model",
        "date",
        "lims_qc",
        "run",
        "well",
        "instrument",
        "movie_length",
        "tag",
        "sample_accession",
        "run_accession",
        "library_load_name",
        "reads",
        "bases",
        "mean",
        "n50",
        "species",
        "loading_conc",
        "binding_kit",
        "sequencing_kit",
        "include_kinetics",
    )

    alt_column_names = {
        "insert_size": "InsertSize",
        "movie_length": "MovieLength",
        "movie_name": "movie",
        "n50": "N50",
        "project": "group",
        "reads": "n",
        "sample": "sanger_id",
        "sample_accession": "accession_number",
        "bases": "sum",
    }

    json_row_data, json_csv_file = process_file(
        headers, alt_column_names, json_input, "json"
    )
    rprt_row_data, rprt_csv_file = process_file(
        headers, alt_column_names, rprt_input, "rprt"
    )

    json_dict = merge_by_idx(json_row_data)
    rprt_dict = merge_by_idx(rprt_row_data)

    idx_count = {}
    count_keys(idx_count, json_dict)
    count_keys(idx_count, rprt_dict)
    for idx in idx_count:
        json_dat = json_dict.get(idx)
        rprt_dat = rprt_dict.get(idx)
        if json_dat and rprt_dat:
            if diffs := diff(json_dat, rprt_dat):
                if diff_fields:
                    print(f"Diff for index '{idx}':\n" + format_rows(*diffs))
                else:
                    print(
                        f"Diff for index '{idx}':\n" + format_rows(json_dat, rprt_dat)
                    )
        elif json_dat:
            print("Only in data.json:\n" + format_rows(json_dat))
        else:
            print("Only in PacBio run report:\n" + format_rows(rprt_dat))


def show_key_count(file):
    data = json.loads(file.read_text())
    counts = {}
    max_count = 0
    for row in data:
        for k, v in row.items():
            if v is not None and v != "":
                c = counts[k] = 1 + counts.get(k, 0)
                if c > max_count:
                    max_count = c

    def hoist_acgt(item):
        k = item[0]
        hoist = 0 if k in "acgt" else 1
        return hoist, k

    click.echo(f"Key counts for '{file}':")
    for k, v in sorted(counts.items(), key=hoist_acgt):
        if v == max_count:
            click.echo(click.style(f"{v:7d}", bold=True) + f"  {k}")
        else:
            click.echo(f"{v:7d}  {k}")


def count_keys(count_dict, dict):
    for k, v in dict.items():
        if v is not None and v != "":
            count_dict[k] = 1 + count_dict.get(k, 0)

    return count_dict


def process_file(headers, alt_column_names, file, source):
    data = json.loads(file.read_text())

    row_data = []
    for row in data:
        row_data.append(process_row(headers, alt_column_names, source, row))

    csv_file = store_csv(file, headers, row_data)

    return row_data, csv_file


def store_csv(file, headers, data):
    csv_file = file.with_suffix(".csv")
    with csv_file.open(mode="w") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for row in data:
            writer.writerow(row[k] for k in headers)
    return csv_file


def merge_by_idx(row_data):
    idx_dat = {}
    for dat in row_data:
        idx = dat["idx"]
        if other_dat := idx_dat.get(idx):
            if dat["specimen"] in other_dat["specimen"]:
                idx_dat[idx] = dat
            elif other_dat["specimen"] in dat["specimen"]:
                pass
            else:
                msg = f"More than one row with index '{idx}':\n" + format_rows(
                    # *diff(dat, other_dat)
                    dat,
                    other_dat,
                )
                print(msg)
                # raise ValueError(msg)
        else:
            idx_dat[idx] = dat

    return idx_dat


def format_rows(*rows):
    header = tuple(rows[0].keys())
    max_hdr = max(len(col) for col in header)
    max_row = []  # Max length of any value in each row
    for r in rows:
        max_v = max(len(str(val)) for val in r.values())
        max_row.append(max_v)
    s = ""
    for col in header:
        s += f"  {col:>{max_hdr}}"
        for i, row in enumerate(rows):
            val = row[col]
            val = "" if val is None else str(val)
            s += f"  {val:{max_row[i]}}"
        s += "\n"

    return s


def diff(a, b):
    """
    Diff two dicts, returning two dicts of the differing values,
    or None if they are the same.
    Assumes that the values are not nested data structures.
    """
    a_diff = {}
    b_diff = {}
    for k in a | b:
        va = a.get(k)
        vb = b.get(k)
        if va != vb:
            a_diff[k] = va
            b_diff[k] = vb

    if a_diff:
        return a_diff, b_diff
    else:
        return None


def process_row(headers, alt_column_names, source, row):
    dat = {}
    for col in headers:
        if col == "source":
            dat[col] = source
        elif alt := alt_column_names.get(col):
            dat[col] = row.get(col, row.get(alt))
        else:
            dat[col] = row.get(col)

    dat["idx"] = make_index(dat)

    return dat


def make_index(row):
    """
    movie + tag_index if present to index each data item
    """
    idx = row.get("movie_name", "unknown")
    if ti := row.get("tag_index"):
        idx += f"#{ti}"
    elif tag := row.get("tag"):
        if m := re.search(r"(\d+)", tag):
            idx += f"#{m.group(1)}"
        else:
            msg = f"Cannot parse index from tag {tag} in:\n" + format_rows(row)
            raise ValueError(msg)

    return idx


def json_file_key_counts(file):
    data = json.loads(pathlib.Path(file).read_text())
    stats = {}
    for row in data:
        for key in row:
            stats[key] = 1 + stats.get(key, 0)
    return json.dumps(stats, sort_keys=True, indent=2)


if __name__ == "__main__":
    cli()
