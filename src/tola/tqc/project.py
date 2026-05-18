import sys
from typing import Literal

import click
from tol.core.datasource_filter import DataSourceFilter

from tola import click_options
from tola.ndjson import ndjson_row
from tola.pretty import s
from tola.terminal import (
    colour_pager,
    dry_warning,
    pretty_dict_itr,
)
from tola.tolqc_client import TolClient
from tola.tqc.engine import id_iterator

MODE = Literal["ADD", "REM"] | None


@click.command()
@click.pass_obj
@click.option(
    "--table",
    "-t",
    type=click.Choice(
        ["data", "specimen", "species"],
        case_sensitive=True,
    ),
    default="data",
    show_default=True,
    help="Name of the table in which the input IDs are found.",
)
@click.option(
    "--add-project",
    "add_options",
    multiple=True,
    help="""
      Names of projects to add each of the input IDs to.  Can be speficied
      multiple times or as a comma-separated list.
    """,
)
@click.option(
    "--remove-project",
    "rem_options",
    multiple=True,
    help="""
      Names of projects to remove each of the input IDs from.  Can be
      speficied multiple times or as a comma-separated list.
    """,
)
@click_options.file
@click_options.file_format
@click_options.apply_flag
@click_options.id_list
def project(
    client,
    table,
    file_list,
    file_format,
    add_options,
    rem_options,
    apply_flag,
    id_list,
):
    """
    Show and edit links from `data` to `project`.

    ID_LIST can be supplied on the command line, or supplied in files given by
    `--file` arguments, or on STDIN.

    Called without `--add-project` or `--remove-project` arguments will show
    the currently allocated projects for each `data` table row found by the
    list of input IDs.
    """

    all_project_names = fetch_all_project_names(client)
    mode, projects = process_add_rem_options(
        all_project_names, add_options, rem_options
    )

    key = f"{table}.id"
    match table:
        case "data":
            id_field = "id"
        case "specimen":
            id_field = "sample.specimen.id"
        case "species":
            id_field = "sample.specimen.species.id"

    id_list = sorted(set(id_iterator(key, id_list, file_list, file_format)))

    db_values = []
    if id_list:
        for chunk in client.pages(id_list):
            fetch_db_values(client, id_field, chunk, db_values)
    else:
        sys.exit(0)

    header = None
    footer = None

    if mode == "ADD":
        db_values = filter_add_project_allocations(projects, db_values)
        if apply_flag:
            header = "Added projects to {} data row{}:"
            add_project_allocations(client, db_values)
        else:
            header = "Found {} data row{} to add projects to:"
            footer = dry_warning(len(db_values))
    elif mode == "REM":
        db_values = filter_rem_project_allocations(projects, db_values)
        if apply_flag:
            header = "Removed projects from {} data row{}:"
            rem_project_allocations(client, db_values)
        else:
            header = "Found {} data row{} to remove projects from:"
            footer = dry_warning(len(db_values))
    else:
        header = "Project allocations for {} data row{}:"

    db_values = project_set_to_sorted_list(db_values)
    if sys.stdout.isatty():
        colour_pager(pretty_dict_itr(db_values, key, head=header, tail=footer))
    else:
        for row in db_values:
            sys.stdout.write(ndjson_row(row))


def process_add_rem_options(
    all_project_names: set[str],
    add_options: list[str],
    rem_options: list[str],
) -> tuple[MODE, set[str]]:
    if add_options and rem_options:
        sys.exit("Error: Specify --add-projects or --remove-projects but not both")
    if add_options:
        mode = "ADD"
        project_set = set_from_comma_list(add_options)
    elif rem_options:
        mode = "REM"
        project_set = set_from_comma_list(rem_options)
    else:
        return None, set()

    if none_such := project_set - all_project_names:
        no_such_projects = ", ".join(f"'{x}'" for x in sorted(none_such))
        sys.exit(f"No such project{s(none_such)}: {no_such_projects}")

    return mode, project_set


def filter_add_project_allocations(projects: set[str], db_values: list[dict]):
    filt_values = []
    for row in db_values:
        if missing := projects - row["projects"].keys():
            filt_values.append({**row, "projects": missing})

    return filt_values


def add_project_allocations(client: TolClient, db_values: list[dict]):
    ads = client.ads
    cdo = client.build_cdo
    new_allocations = []
    for row in db_values:
        data_id = row["data.id"]
        for project_id in row["projects"]:
            new_allocations.append(
                cdo(
                    "allocation",
                    None,
                    {
                        "project_id": project_id,
                        "data_id": data_id,
                    },
                )
            )

    for chunk in client.pages(new_allocations):
        ads.insert("allocation", chunk)


def filter_rem_project_allocations(projects: set[str], db_values: list[dict]):
    filt_values = []
    for row in db_values:
        proj_dict = row["projects"]
        if remove := projects & row["projects"].keys():
            rem_dict = {x: proj_dict[x] for x in remove}
            filt_values.append({**row, "projects": rem_dict})

    return filt_values


def rem_project_allocations(client: TolClient, db_values: list[dict]):
    allocation_ids = []
    for row in db_values:
        allocation_ids.extend(row["projects"].values())

    ads = client.ads
    for chunk in client.pages(allocation_ids):
        ads.delete("allocation", chunk)


def set_from_comma_list(input: list[str]) -> set[str]:
    name_set = set()
    for val in input:
        for name in val.split(","):
            name_set.add(name)
    return name_set


def fetch_all_project_names(client: TolClient) -> set[str]:
    return {x.id for x in client.ads_ro.get_list("project")}


def fetch_db_values(
    client: TolClient,
    id_field: str,
    id_list: list[str] | None,
    db_values: list[dict],
):
    filt = None if id_list is None else DataSourceFilter(in_list={id_field: id_list})
    for data in client.ads_ro.get_list(
        "data",
        object_filters=filt,
        requested_fields=[
            "id",
            "project_assn",
            "sample.id",
            "sample.specimen.id",
            "sample.specimen.species.id",
        ],
    ):
        projects = {x.project.id: x.id for x in data.project_assn}
        specimen = None
        species = None
        if (smpl := data.sample) and (spmn := smpl.specimen):
            specimen = spmn.id
            if spcs := spmn.species:
                species = spcs.id
        db_values.append(
            {
                "species.id": species,
                "specimen.id": specimen,
                "data.id": data.id,
                "projects": projects,
            }
        )


def project_set_to_sorted_list(db_values: list[dict]) -> list[dict]:
    """
    Returns a new list of rows with the `projects` dict replaced by a sorted
    list.
    """
    new = []
    for row_in in db_values:
        row_out = {}
        for k, v in row_in.items():
            if k == "projects":
                row_out[k] = sorted(v)
            else:
                row_out[k] = v
        new.append(row_out)
    return new
