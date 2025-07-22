import json
import logging
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import ZipFile

import click
from partisan.irods import DataObject

from tola import click_options
from tola.ndjson import get_input_objects, ndjson_row
from tola.store_folder import upload_files
from tola.tqc.engine import irods_path_dataobject, update_file_size_and_md5_if_missing


@click.command
@click.pass_context
@click.option(
    "--auto",
    "auto_flag",
    flag_value=True,
    default=False,
    show_default=True,
    help="""
      Query ToLQC for PacBio run metrics missing folders to work on using the
      `work-pacbio-run-metrics-folders` report
    """,
)
@click_options.input_files
def load_pacbio_images(ctx, input_files, auto_flag):
    """
    Add folders of images from the `<RUN_ID>.reports.zip` file on iRODS to the
    `pacbio_run_metrics` table.
    """

    client = ctx.obj

    input_objects = report_iter(client) if auto_flag else get_input_objects(input_files)
    run_ids_loaded = set()
    for spec in input_objects:
        if data := load_pacbio_metrics_images(client, run_ids_loaded, spec):
            sys.stdout.write(ndjson_row(data))


def report_iter(client):
    for row in client.stream_lines(
        "report/work-pacbio-run-metrics-folders",
        {
            "folder_ulid": None,
            "format": "NDJSON",
        },
    ):
        yield json.loads(row)


def load_pacbio_metrics_images(client, run_ids_loaded, spec):
    if run_id := spec.get("run_id"):
        spec["pacbio_run_metrics.id"] = run_id
    else:
        run_id = spec["pacbio_run_metrics.id"]

    seq_file = spec["remote_path"]

    seq_path, irods_seq = irods_path_dataobject(seq_file)
    if irods_seq:
        update_file_size_and_md5_if_missing(client, spec, seq_path)

    if run_id in run_ids_loaded:
        return
    run_ids_loaded.add(run_id)

    reports_zip = seq_path.parent / f"{run_id}.reports.zip"
    if irods_seq:
        reports_zip = DataObject(reports_zip)
    if not reports_zip.exists():
        logging.info(f"No such file: {reports_zip}")
        return
    logging.info(f"Found: {reports_zip}")

    tmp_dir = TemporaryDirectory()
    local_path = Path(tmp_dir.name)
    local_zip_path = local_path / reports_zip.name
    reports_zip.get(local_zip_path, verify_checksum=True)
    local_zip = ZipFile(local_zip_path)
    local_zip.extractall(local_path)
    spec["directory"] = local_path

    return upload_files(client, "pacbio_run_s3", "pacbio_run_metrics", spec)
