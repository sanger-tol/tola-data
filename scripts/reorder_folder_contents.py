#!/usr/bin/env python3

import re
import sys

import click
from tol.core import DataSourceFilter

from tola import click_options
from tola.ndjson import ndjson_row
from tola.store_folder import FilePattern
from tola.tolqc_client import TolClient


@click.command
@click.option(
    "--folder-location",
    help="folder_location.id",
    required=True,
)
@click_options.tolqc_alias
def main(tolqc_alias, folder_location):
    client = TolClient(tolqc_alias=tolqc_alias)
    ads = client.ads_ro
    (fldr_loc,) = ads.get_by_ids("folder_location", [folder_location])
    if not fldr_loc:
        click.echo(f"No such folder_location: {folder_location!r}", err=True)
        sys.exit(1)

    img_key = "image_file_patterns"
    image_patterns = fldr_loc.files_template.get(img_key)
    if not image_patterns:
        click.echo(
            (
                f"Missing expected value for '{img_key}'"
                f" in {folder_location}.files_template"
            ),
            err=True,
        )
        sys.exit(1)
    image_patterns = [FilePattern(is_image=True, **x) for x in image_patterns]

    if not all(x.index is not None for x in image_patterns):
        click.echo(
            f"All image patterns must have an 'index' key:\n{image_patterns!r}",
            err=True,
        )
        sys.exit(1)

    for fldr in ads.get_list(
        "folder",
        object_filters=DataSourceFilter(
            exact={
                "folder_location.id": folder_location,
            }
        ),
    ):
        search = image_patterns.copy()
        if not (before := fldr.image_file_list):
            continue
        matched = {}
        for spec in before:
            for i, pat in enumerate(search):
                if re.fullmatch(pat.pattern, spec["file"]):
                    search.pop(i)
                    matched[pat.index] = spec
                    break
        after = [matched[i] for i in sorted(matched)]
        if len(before) != len(after):
            click.echo(
                f"Failed to match all files in {fldr.id}:\n  {before}\ngot:\n  {after}"
            )
            sys.exit(1)
        if before != after:
            sys.stdout.write(
                ndjson_row(
                    {
                        "folder.id": fldr.id,
                        "image_file_list": after,
                    }
                )
            )


if __name__ == "__main__":
    main()
