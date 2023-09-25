import csv
import json
import pathlib
import re
import sys


def main():
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

    json_row_data, json_csv_file = process_file(
        headers, "pacbio_tolqc_data.json", "json"
    )
    rprt_row_data, rprt_csv_file = process_file(
        headers, "pacbio_run_report.json", "rprt"
    )

    json_dict = merge_by_idx(json_row_data)
    rprt_dict = merge_by_idx(rprt_row_data)

    idx_count = count_keys(json_dict)
    count_keys(rprt_dict, idx_count)
    for idx in idx_count:
        json_dat = json_dict.get(idx)
        rprt_dat = rprt_dict.get(idx)
        if json_dat and rprt_dat:
            if diffs := diff(json_dat, rprt_dat):
                # print(f"Diff for index '{idx}':\n" + format_rows(*diffs))
                print(f"Diff for index '{idx}':\n" + format_rows(json_dat, rprt_dat))
        elif json_dat:
            print("Only in data.json:\n" + format_rows(json_dat))
        else:
            print("Only in PacBio run report:\n" + format_rows(rprt_dat))


def count_keys(dict, count_dict=None):
    if not count_dict:
        count_dict = {}
    for k in dict:
        count_dict[k] = 1 + count_dict.get(k, 0)

    return count_dict


def process_file(headers, filename, source):
    file = pathlib.Path(filename)
    data = json.loads(file.read_text())
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

    row_data = []
    for row in data:
        row_data.append(process_row(headers, source, row, alt_column_names))

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


def process_row(headers, source, row, alt_column_names):
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


if __name__ == '__main__':
    main()

"""

Tag occupation in ToL QC data.json

    tolqc = {
        "ExtensionTime": 2552,
        "InsertSize": 2552,
        "L100": 7202,
        "L50": 7202,
        "L60": 7202,
        "L70": 7202,
        "L80": 7202,
        "L90": 7202,
        "MovieLength": 2552,
        "N100": 7202,
        "N50": 7202,
        "N60": 7202,
        "N70": 7202,
        "N80": 7202,
        "N90": 7202,
        "a": 7208,
        "accession_number": 6998,
        "barcode": 7208,
        "boldcheck": 2706,
        "boldstats": 4454,
        "c": 7208,
        "date": 7208,
        "dups": 7208,
        "exp_accession": 4014,
        "filtered": 7208,
        "g": 7208,
        "group": 7182,
        "input": 7208,
        "instrument": 7208,
        "largest": 7204,
        "library_load_name": 6996,
        "match": 4796,
        "mean": 7204,
        "model": 7208,
        "movie": 7208,
        "n": 7204,
        "pipeline": 7208,
        "pipeline_id_lims": 6996,
        "platform": 7208,
        "plot-all_readlength_hist": 908,
        "plot-base_yield": 6165,
        "plot-bq_histogram": 992,
        "plot-ccs_accuracy_hist": 3753,
        "plot-ccs_all_readlength_hist": 2876,
        "plot-ccs_hifi_read_length_yield": 2845,
        "plot-ccs_npasses_hist": 3761,
        "plot-ccs_readlength_hist": 3753,
        "plot-concordance": 5999,
        "plot-hexbin_length": 6165,
        "plot-insertLenDist0": 2437,
        "plot-interAdapterDist0": 6189,
        "plot-m5c_detections": 1737,
        "plot-m5c_detections_hist": 1729,
        "plot-nreads": 989,
        "plot-nreads_histogram": 989,
        "plot-raw_read_length": 6166,
        "plot-readLenDist0": 6169,
        "plot-readlength": 5999,
        "plot-readlength_histogram": 989,
        "plot-readlength_qv_hist2d.hexbin": 3784,
        "plot-subread_lengths": 2412,
        "run": 6998,
        "run_accession": 4014,
        "rundir": 6998,
        "sanger_id": 6996,
        "smallest": 7204,
        "species": 7208,
        "species_lims": 6996,
        "species_name": 7208,
        "specimen": 7208,
        "study_accession": 4014,
        "submission_date": 4014,
        "sum": 7204,
        "t": 7208,
        "tag_index": 4875,
        "tag_sequence": 6996,
        "type": 7208,
        "well_label": 6998,
    }

"""
