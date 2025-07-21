import json

import click

from tola import click_options
from tola.ndjson import get_input_objects, ndjson_row


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
    ads = client.ads
    cdo = client.build_cdo

    input_objects = report_iter(client) if auto_flag else get_input_objects(input_files)


def report_iter(client):
    for row in client.stream_lines(
        "report/work-pacbio-run-metrics-folders",
        {
            "folder_ulid": None,
            "format": "NDJSON",
        },
    ):
        yield json.loads(row)
