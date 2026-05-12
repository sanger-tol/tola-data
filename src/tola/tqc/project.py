import sys

import click
from tol.core.datasource_filter import DataSourceFilter

from tola import click_options
from tola.ndjson import ndjson_row
from tola.terminal import (
    colour_pager,
    dry_warning,
    pretty_changes_itr,
    pretty_dict_itr,
)
from tola.tqc.engine import (
    convert_type,
    core_data_object_to_dict,
    dicts_to_core_data_objects,
    fetch_list_or_exit,
    id_iterator,
    input_objects_or_exit,
)


@click.command()
@click.pass_obj
@click_options.id_list
@click_options.apply_flag
def project(
    client,
    id_list,
    apply_flag,
):
    """
    Show and edit links from `data` to `project`
    """
    filt = DataSourceFilter(in_list={"id": id_list}) if id_list else None
    for data in client.ads_ro.get_list(
        "data",
        object_filters=filt,
        requested_fields=["project_assn"],
    ):
        projects = sorted([x.project.id for x in data.project_assn])
        # projects = data.sample.id
        sys.stdout.write(
            ndjson_row(
                {
                    "data": data.id,
                    "projects": projects,
                }
            )
        )
