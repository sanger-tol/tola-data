import sys
from subprocess import CalledProcessError

import click

from tola import click_options, tolqc_client
from tola.illumina_images import PlotBamStatsRunner
from tola.ndjson import get_input_objects, ndjson_row
from tola.store_folder import upload_files


@click.command
@click_options.tolqc_alias
@click_options.tolqc_url
@click_options.api_token
@click_options.input_files
def cli(tolqc_alias, tolqc_url, api_token, input_files):
    """
    Runs plot-bamstats (in a temporary directory) on BAM stats files, uploads
    the images to the S3 location given in the "illumina_data_s3"
    `FolderLocation` and stores a folder in the ToLQC database to record
    their location.

    Input is ND-JSON, each line of which is structured like this example:

      \b
      {
        "data.id": "49524_4#6",
        "bam_file": "irods:/seq/illumina/runs/49/49524/lane4/plex6/49524_4#6.cram",
        "library_type": "Hi-C - Arima v2"
      }

    and can be either a list of files or STDIN.

    Files will be fetched from irods if the "bam_file" path begins with "irods:"

    The stats file is expected to be alongside the BAM file with the
    suffix "_F0xB00.stats", so in the above example would be expected to be:

      irods:/seq/illumina/runs/49/49524/lane4/plex6/49524_4#6_F0xB00.stats

    Requires the Samtools `plot-bamstats` executable.
    """

    client = tolqc_client.TolClient(tolqc_url, api_token, tolqc_alias, page_size=100)
    input_objects = get_input_objects(input_files)
    runner = PlotBamStatsRunner()
    for obj in input_objects:
        try:
            images = runner.run_bamstats_in_tmpdir(obj["bam_file"])
        except CalledProcessError:
            sys.stderr.write(f"Error running plot-bamstats on: {obj}\n")
            continue
        obj["directory"] = images.dir_path
        sys.stdout.write(
            ndjson_row(upload_files(client, "illumina_data_s3", "data", obj))
        )


if __name__ == "__main__":
    cli()
