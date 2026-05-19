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

MODE = Literal["ADD", "REM", "PRI"] | None


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
      Names of projects to add each of the input IDs to.  Can be given
      multiple times and as a comma-separated list.
    """,
)
@click.option(
    "--remove-project",
    "rem_options",
    multiple=True,
    help="""
      Names of projects to remove each of the input IDs from.  Can be given
      multiple times and as a comma-separated list.
    """,
)
@click.option(
    "--primary-project",
    "primary_project_name",
    help="""
      For each input ID, add this project if missing and set it to the primary
      project.
    """,
)
@click.option(
    "--force-primary",
    "force_primary_flag",
    flag_value=True,
    help="""
      Allow switching of the primary project with the `--primary-project`
      option.
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
    primary_project_name,
    force_primary_flag,
    apply_flag,
    id_list,
):
    """
    Show and edit links from `data` to `project`.

    ID_LIST can be supplied on the command line, or supplied in files given by
    `--file` arguments, or on STDIN.

    The `--add-project`, `--remove-project` and `--primary-project` options
    are mutually exclusive.  If none of these are given, the currently
    allocated projects for each `data` table row found by the list of input
    IDs will be shown.
    """

    all_project_names = fetch_all_project_names(client)
    mode, projects = process_add_rem_options(
        all_project_names,
        add_options,
        rem_options,
        primary_project_name,
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
            header = "Added projects to {} data table row{}:"
            add_project_allocations(client, db_values)
        else:
            header = "Found {} data table row{} to add projects to:"
            footer = dry_warning(len(db_values))
    elif mode == "REM":
        db_values = filter_rem_project_allocations(projects, db_values)
        if apply_flag:
            header = "Removed projects from {} data table row{}:"
            rem_project_allocations(client, db_values)
        else:
            header = "Found {} data table row{} to remove projects from:"
            footer = dry_warning(len(db_values))
    elif mode == "PRI":
        db_values, unset_values = filter_pri_project_allocations(
            projects, db_values, force_primary_flag
        )
        if apply_flag:
            header = "Added or set primary project allocation to {} data table row{}:"
            pri_project_allocations(client, db_values, unset_values, projects)
        else:
            header = "Found {} data table row{} to add primary project allocation to:"
            footer = dry_warning(len(db_values))
    else:
        header = "Project allocations for {} data table row{}:"

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
    primary_project_name: str,
) -> tuple[MODE, set[str]]:
    if add_options and rem_options:
        sys.exit("Error: Specify --add-project or --remove-project but not both")
    elif add_options and primary_project_name:
        sys.exit("Error: Specify --add-project or --primary-project but not both")
    elif rem_options and primary_project_name:
        sys.exit("Error: Specify --remove-project or --primary-project but not both")

    if add_options:
        mode = "ADD"
        project_set = set_from_comma_list(add_options)
    elif rem_options:
        mode = "REM"
        project_set = set_from_comma_list(rem_options)
    elif primary_project_name:
        mode = "PRI"
        project_set = set_from_comma_list([primary_project_name])
        if len(project_set) != 1:
            sys.exit(
                "Error: Expected a single project name in"
                f" --primary-project {primary_project_name!r}"
            )
    else:
        return None, set()

    if none_such := project_set - all_project_names:
        no_such_projects = set_to_sorted_string(none_such)
        sys.exit(f"No such project{s(none_such)}: {no_such_projects}")

    return mode, project_set


def filter_add_project_allocations(
    projects: set[str], db_values: list[dict]
) -> list[dict]:
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


def filter_rem_project_allocations(
    projects: set[str], db_values: list[dict]
) -> list[dict]:
    filt_values = []
    for row in db_values:
        proj_dict = row["projects"]
        if remove := projects & row["projects"].keys():
            rem_dict = {x: proj_dict[x] for x in remove}
            rem_row = {k: v for k, v in row.items() if k != "primary"}
            rem_row["projects"] = rem_dict
            filt_values.append(rem_row)

    return filt_values


def rem_project_allocations(client: TolClient, db_values: list[dict]):
    allocation_ids = []
    for row in db_values:
        allocation_ids.extend(row["projects"].values())

    ads = client.ads
    for chunk in client.pages(allocation_ids):
        ads.delete("allocation", chunk)


def filter_pri_project_allocations(
    projects: set[str], db_values: list[dict], force_flag: bool
) -> tuple[list[dict], list[str]]:
    proj_name = list(projects)[0]
    filt_values = []
    unset_values = []
    others = set()

    skip = {"projects", "primary"}

    for row in db_values:
        chng = {}

        # Add primary project if missing
        proj_dict = row["projects"]
        if proj_name not in proj_dict:
            chng["projects"] = projects

        pri_val = row["primary"]
        if pri_val is not None:
            # Unpack the single item dict
            ((othr_name, alloc_id),) = list(pri_val.items())
            if othr_name != proj_name:
                if force_flag:
                    unset_values.append(alloc_id)
                    # Unset `pri_val` to trigger the new primary project to be
                    # stored.
                    pri_val = None
                else:
                    others.add(othr_name)

        # Set as primary primary project when empty
        if pri_val is None:
            if alloc_id := proj_dict.get(proj_name):
                # Get the allocation.id so that we can update the row
                pri_val = {proj_name: alloc_id}
            else:
                pri_val = projects
            chng["primary"] = pri_val

        if chng:
            upd = {k: v for k, v in row.items() if k not in skip}
            for k, v in chng.items():
                upd[k] = v
            filt_values.append(upd)

    if others:
        primary_proj = set_to_sorted_string(projects)
        other_primary = set_to_sorted_string(others)
        sys.exit(
            f"Error: Setting primary to {primary_proj}, but some data table entries"
            f" have these primary project allocations: {other_primary}\n"
            "Use `--force-primary` to switch primary projects."
        )

    return filt_values, unset_values


def pri_project_allocations(
    client: TolClient,
    db_values: list[dict],
    unset_values: list[str],
    projects: set[str],
):
    primary_project_id = list(projects)[0]
    ads = client.ads
    cdo = client.build_cdo

    # Set old `allocation.is_primary` values to null
    unset_primary = []
    for alloc_id in unset_values:
        unset_primary.append(
            cdo(
                "allocation",
                alloc_id,
                {
                    "is_primary": None,
                },
            )
        )

    for chunk in client.pages(unset_primary):
        ads.upsert("allocation", chunk)

    allocations = []
    for row in db_values:
        data_id = row["data.id"]

        if "projects" in row:
            # Add a new row
            allocations.append(
                cdo(
                    "allocation",
                    None,
                    {
                        "project_id": primary_project_id,
                        "data_id": data_id,
                        "is_primary": True,
                    },
                )
            )
        else:
            # Update an existing row
            allocations.append(
                cdo(
                    "allocation",
                    row["primary"][primary_project_id],
                    {
                        "is_primary": True,
                    },
                )
            )

    for chunk in client.pages(allocations):
        ads.upsert("allocation", chunk)


def set_from_comma_list(opts: list[str]) -> set[str]:
    name_set = set()
    for val in opts:
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
        primary = None
        for x in data.project_assn:
            if x.is_primary:
                primary = {x.project.id: x.id}
                break
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
                "primary": primary,
            }
        )


def set_to_sorted_string(input: set[str]) -> str:
    return ", ".join(f"{x!r}" for x in sorted(input))


def project_set_to_sorted_list(db_values: list[dict]) -> list[dict]:
    """
    Returns a new list of rows with the `projects` and `primary` dicts
    replaced by a sorted list.
    """
    new = []
    for row_in in db_values:
        row_out = {}
        for k, v in row_in.items():
            if k == "projects":
                row_out[k] = sorted(v)
            elif k == "primary":
                row_out[k] = None if v is None else sorted(v)[0]
            else:
                row_out[k] = v
        new.append(row_out)
    return new
