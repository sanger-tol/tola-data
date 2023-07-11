# SPDX-FileCopyrightText: 2023 Genome Research Ltd.
#
# SPDX-License-Identifier: MIT

import inspect
import json
import os
import re
import sys
import pathlib

from main.model import Project
from sqlalchemy import select
from tol.api_client import ApiDataSource, ApiObject
from tola import db_connection


def main(conf_file="tol_track.conf"):
    proj_iter = conf_file_Projects(pathlib.Path(conf_file))
    if False:
        store_Projects_via_sql_alchemy(proj_iter)
    else:
        store_Projects_via_tol_api(proj_iter)


def store_Projects_via_sql_alchemy(proj_iter):
    engine, Session = db_connection.tola_db_engine(echo=True)

    with Session() as ssn:
        for proj in proj_iter:
            query = select(Project).filter_by(lims_id=proj.lims_id)
            if db_proj := ssn.scalars(query).one_or_none():
                proj.project_id = db_proj.project_id
                ssn.merge(proj)
            else:
                ssn.add(proj)

        # ssn.flush()
        ssn.commit()


def store_Projects_via_tol_api(proj_iter):
    tolqc = ApiDataSource(
        {
            'url': os.getenv('TOLQC_URL') + '/api/v1',
            'key': os.getenv('TOLQC_API_KEY'),
        }
    )
    stored_projects = {p.lims_id: p for p in tolqc.get_list('projects')}
    for proj in proj_iter:
        print(f"Processing project with lims_id = {proj.lims_id}")
        if db_proj := stored_projects.get(proj.lims_id):
            db_proj.hierarchy_name = proj.hierarchy_name
            db_proj.description = proj.description
            tolqc.update(db_proj)
        else:
            api_proj = ApiObject(
                'projects',
                None,
                {
                    "hierarchy_name": proj.hierarchy_name,
                    "description": proj.description,
                    "lims_id": proj.lims_id,
                },
            )
            tolqc.create(api_proj)


def conf_file_Projects(conf_file):
    for line in conf_file.open():
        if re.match(r"^\s*#", line):
            continue
        if not re.search(r"\w", line):
            continue
        study_id, hierarchy, description = line.rstrip("\r\n").split("\t")
        hierarchy = re.sub(r"%TXGROUP%", "{}", hierarchy)
        yield Project(
            hierarchy_name=hierarchy,
            lims_id=int(study_id),
            description=description,
        )
        # yield study_id, hierarchy, description


if __name__ == "__main__":
    main(*sys.argv[1:])

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
