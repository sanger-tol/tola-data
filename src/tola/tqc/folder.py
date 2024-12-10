import json
import sys

import click

from tola import click_options
from tola.ndjson import ndjson_row
from tola.pretty import colour_pager
from tola.store_folder import upload_files
from tola.tqc.engine import (
    input_objects_or_exit,
    pretty_dict_itr,
)


@click.command()
@click.pass_context
@click_options.table
@click.option(
    "--location",
    help="Name of folder_location.id",
    required=True,
)
@click_options.input_files
def store_folders(ctx, table, location, input_files):
    """
    Upload files to S3 storage. Each row of the ND-JSON format INPUT_FILES
    must contain a primary key value for the table, a `directory` entry with
    the path to the local directory containging the files to be uploaded,
    plus any key/value pairs for named format specifiers in the captions in
    the template. e.g.

        {

            "pacbio_run_metrics.id": "m84098_240508_102324_s2",

            "directory": "",

            "specimen": "mBalPhy2"

        }

    where the captions templates in `folder_location.files_template` contain `
    {specimen}` strings.

    """

    client = ctx.obj

    input_obj = input_objects_or_exit(ctx, input_files)
    stored_folders = []
    error = None
    for spec in input_obj:
        try:
            if fldr := upload_files(
                client,
                folder_location_id=location,
                table=table,
                spec=spec,
            ):
                stored_folders.append(fldr)
        except Exception as excn:  # noqa: BLE001
            error = spec, excn
            break

    if stored_folders:
        if sys.stdout.isatty():
            colour_pager(
                pretty_dict_itr(
                    stored_folders,
                    "folder.id",
                    head="Stored {} folder{}:",
                )
            )
        else:
            for fldr in stored_folders:
                sys.stdout.write(ndjson_row(fldr))

    if error:
        spec, excn = error
        sys.exit(
            f"{excn.__class__.__name__}: "
            + "; ".join(excn.args)
            + " when storing:\n"
            + json.dumps(spec, indent=4)
        )
