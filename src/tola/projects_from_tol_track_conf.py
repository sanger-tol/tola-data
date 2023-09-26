# SPDX-FileCopyrightText: 2023 Genome Research Ltd.
#
# SPDX-License-Identifier: MIT

import click
import re
import pathlib
import tola.marshals

from main.model import Project


@click.command(help="Update list of Projects from tol_track.conf (or similar) file")
@click.argument(
    "file_list",
    nargs=-1,
    type=click.Path(
        dir_okay=False,
        exists=True,
        readable=True,
        path_type=pathlib.Path,
    ),
)
@tola.marshals.mrshl
def main(mrshl, file_list):
    for file in file_list:
        load_project_conf_file(mrshl, file)
    mrshl.commit()


def load_project_conf_file(mrshl, file):
    for spec in conf_file_project_specs(file):
        mrshl.update_or_create(Project, spec, ("lims_id",))


def conf_file_project_specs(conf_file):
    for line in conf_file.open():
        if re.match(r"^\s*#", line):
            continue
        if not re.search(r"\w", line):
            continue
        study_id, hierarchy, description = line.rstrip("\r\n").split("\t")
        hierarchy = re.sub(r"%TXGROUP%", "{}", hierarchy)
        yield {
            "hierarchy_name": hierarchy,
            "lims_id": int(study_id),
            "description": description,
        }


if __name__ == "__main__":
    main()

# from main.schema import ProjectSchema
# schema = ProjectSchema()
# for proj in proj_iter:
#     # new_proj = schema.load(schema.dump(proj))
#     obj = ApiObject(
#         'projects',
#         None,
#         {
#             "hierarchy_name": proj.hierarchy_name,
#             "description": proj.description,
#             "lims_id": proj.lims_id,
#         },
#     )
#     # print(json.dumps({"data": obj.to_json()}))
#     print(json.dumps(schema.dump(proj)))
#     # print(json.dumps(schema.dump(new_proj)))
#     return
# print(json.dumps(schema.dump(proj_iter, many=True)))
# return
