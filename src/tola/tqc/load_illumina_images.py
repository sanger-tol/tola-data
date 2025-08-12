import logging
import sys
from subprocess import CalledProcessError

import click

from tola import click_options
from tola.illumina_images import NoSuchIrodsFileError, PlotBamStatsRunner
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
@click_options.fetch_input
@click_options.quiet
@click_options.input_files
def load_illumina_images(ctx, input_files, auto_flag, fetch_input, quiet):
    """
    Runs plot-bamstats (in a temporary directory) on BAM stats files, uploads
    the images to the S3 location given in the "illumina_data_s3"
    `FolderLocation` and stores a folder in the ToLQC database to record
    their location.

    Input is ND-JSON, each line of which is structured like this example:

      \b
      {
        "data.id": "49524_4#6",
        "reads": 819928808,
        "bases": 123809250008,
        "file.id": '145847',
        "remote_path": "irods:/seq/illumina/runs/49/49524/lane4/plex6/49524_4#6.cram",
        "size_bytes": 38073830696,
        "md5": "125d8ca94afca6148f332cb194b192ab",
        "library_type": "Hi-C - Arima v2",
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

    if fetch_input:
        auto_flag = True
    input_objects = get_work(client) if auto_flag else get_input_objects(input_files)
    runner = PlotBamStatsRunner()

    if fetch_input:
        for obj in input_objects:
            sys.stdout.write(ndjson_row(obj))
        sys.exit()

    for obj in input_objects:
        if oid := obj.get("data_id"):
            obj["data.id"] = oid
        else:
            oid = obj["data.id"]

        try:
            images = runner.run_bamstats_in_tmpdir(obj["remote_path"])
        except NoSuchIrodsFileError as err:
            msg = "\n".join(err.args)
            logging.warning(f"{msg} on: {ndjson_row(obj)}")
            continue
        except CalledProcessError:
            logging.warning(f"Error running plot-bamstats on: {ndjson_row(obj)}")
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

        if not quiet:
            sys.stdout.write(ndjson_row(data))


def get_work(client):
    """
    Build a complete list of work to do and return it.  Do not yield each
    individual record, because populating `data.folder_ulid` will change the
    list of results from the database query, and the API's automatic paging
    will cause records to be skipped!
    """

    spec_list = []
    for data in client.ads_get_list(
        "data",
        {
            "lims_qc": {
                "eq": {"value": "pass"},
            },
            "run.platform.name": {
                "eq": {"value": "Illumina"},
            },
            "folder_ulid": {
                "exists": {"negate": True},
            },
            "files.remote_path": {
                "exists": {},
            },
            "files.file_type": {
                "eq": {"value": "CRAM"},
            },
        },
    ):
        for file in data.files:
            spec_list.append(
                {
                    "data.id": data.id,
                    "reads": data.reads,
                    "bases": data.bases,
                    "file.id": file.id,
                    "remote_path": file.remote_path,
                    "size_bytes": file.size_bytes,
                    "md5": file.md5,
                    "library_type": data.library.library_type.id,
                }
            )

    return spec_list
