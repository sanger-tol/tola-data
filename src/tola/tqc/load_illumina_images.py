import json
import sys
from subprocess import CalledProcessError

import click

from tola import click_options
from tola.illumina_images import PlotBamStatsRunner
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
      Query ToLQC for Illumina data missing folders to work on using the
      `work-illumina-data-folders` report
    """,
)
@click_options.input_files
def load_illumina_images(ctx, input_files, auto_flag):
    """
    Runs plot-bamstats (in a temporary directory) on BAM stats files, uploads
    the images to the S3 location given in the "illumina_data_s3"
    `FolderLocation` and stores a folder in the ToLQC database to record
    their location.

    Input is ND-JSON, each line of which is structured like this example:

      \b
      {
        "data_id": "49524_4#6",
        "lims_qc": "pass",
        "reads": 819928808,
        "bases": 123809250008,
        "folder_ulid": "01J8HDZDM2F09ZF5YWMBZNPPTE",
        "remote_path": "irods:/seq/illumina/runs/49/49524/lane4/plex6/49524_4#6.cram",
        "size_bytes": 38073830696,
        "md5": "125d8ca94afca6148f332cb194b192ab",
        "file_type": "CRAM",
        "library_type": "Hi-C - Arima v2"
      }

    Files will be fetched from iRODS if the "remote_path" path begins
    with "irods:", in which case if either "size_bytes" or "md5" is missing
    from the input they will be filled in from iRODS metadata.

    The stats file is expected to be alongside the BAM file with the
    suffix "_F0xB00.stats", so in the above example would be expected to be:

      irods:/seq/illumina/runs/49/49524/lane4/plex6/49524_4#6_F0xB00.stats

    If "reads" or "bases" is missing from the input, it will be filled in from
    the stats file.

    Requires the Samtools `plot-bamstats` executable.
    """

    client = ctx.obj
    ads = client.ads
    cdo = client.build_cdo

    input_objects = report_iter(client) if auto_flag else get_input_objects(input_files)
    runner = PlotBamStatsRunner()

    for obj in input_objects:
        if oid := obj.get("data_id"):
            obj["data.id"] = oid
        else:
            oid = obj["data.id"]

        try:
            images = runner.run_bamstats_in_tmpdir(obj["remote_path"])
        except CalledProcessError:
            sys.stderr.write(f"Error running plot-bamstats on: {obj}\n")
            continue
        obj["directory"] = images.dir_path
        images.parse_stats_file()
        data = upload_files(client, "illumina_data_s3", "data", obj)

        if (images.reads is not None and images.bases is not None) and (
            obj.get("reads") is None or obj.get("bases") is None
        ):
            ads.upsert(
                "data",
                [cdo("data", oid, {"reads": images.reads, "bases": images.bases})],
            )
            data["reads"] = images.reads
            data["bases"] = images.bases

        bam_path, irods_obj = irods_path_dataobject(obj["remote_path"])
        if irods_obj:
            update_file_size_and_md5_if_missing(client, obj, irods_obj)

        sys.stdout.write(ndjson_row(data))


def report_iter(client):
    for row in client.stream_lines(
        "report/work-illumina-data-folders",
        {
            "folder_ulid": None,
            "lims_qc": "pass",
            "format": "NDJSON",
        },
    ):
        yield json.loads(row)
