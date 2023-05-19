#!/usr/bin/env python3

import inspect
import pathlib
import re
import sys


def main(table_names):
    for name in table_names:
        snake, camel = snake_and_camel(name)
        templates = file_templates(snake, camel)
        for root, conf in templates.items():
            create_files(root, snake, conf)


def create_files(root, snake, conf):
    root_dir = pathlib.Path(root)
    root_dir.mkdir(exist_ok=True)

    source = root_dir / f"{snake}.py"
    with source.open(mode="x") as source_fh:
        source_fh.writelines(conf["content"] + "\n")

    init = root_dir / "__init__.py"
    with init.open(mode="a") as init_fh:
        init_fh.writelines(conf["init_line"] + "\n")


def snake_and_camel(name):
    words = [x for x in re.findall(r"([A-Z]*[a-z]*)", name) if len(x)]
    snake = "_".join(x.lower() for x in words)
    camel = "".join(x.title() for x in words)
    return snake, camel


def file_templates(snake, camel):
    # header indentation needs to match content in templates dict
    header = """
            # SPDX-FileCopyrightText: 2023 Genome Research Ltd.
            #
            # SPDX-License-Identifier: MIT
            """

    templates = {
        "model": f"""
            {header}

            from tol.api_base.model import LogBase, db, setup_model


            @setup_model
            class {camel}(LogBase):
                __tablename__ = "{snake}"

                class Meta:
                    type_ = "{snake}s"
                    id_column = "{snake}_id"

                {snake}_id = db.Column(db.String(), primary_key=True)
                string_col = db.Column(db.String())
                integer_col = db.Column(db.Integer())
                float_col = db.Column(db.Float())
                other_id = db.Column(db.String(), db.ForeignKey("other.other_id"))

                rel_other = db.relationship("Other", back_populates="{snake}")

            """,
        "resource": f"""
            {header}

            from main.service import {camel}Service
            from main.swagger import {camel}Swagger
            from tol.api_base.resource import AutoResourceGroup, setup_resource_group


            api_{snake} = {camel}Swagger.api


            @setup_resource_group
            class {camel}ResourceGroup(AutoResourceGroup):
                class Meta:
                    service = {camel}Service
                    swagger = {camel}Swagger

            """,
        "schema": f"""
            {header}

            from main.model import {camel}
            from tol.api_base.schema import BaseSchema, setup_schema


            @setup_schema
            class {camel}Schema(BaseSchema):
                class Meta(BaseSchema.BaseMeta):
                    model = {camel}

            """,
        "service": f"""
            {header}

            from main.model import {camel}
            from main.schema import {camel}Schema
            from tol.api_base.service import BaseService, setup_service


            @setup_service
            class {camel}Service(BaseService):
                class Meta:
                    model = {camel}
                    schema = {camel}Schema

            """,
        "swagger": f"""
            {header}

            from main.schema import {camel}Schema
            from tol.api_base.swagger import BaseSwagger, setup_swagger


            @setup_swagger
            class {camel}Swagger(BaseSwagger):
                class Meta:
                    schema = {camel}Schema

            """,
    }

    init_lines = {
        "model": f"from .{snake} import {camel}",
        "resource": f"from .{snake} import api_{snake}",
        "schema": f"from .{snake} import {camel}Schema",
        "service": f"from .{snake} import {camel}Service",
        "swagger": f"from .{snake} import {camel}Swagger",
    }

    return {
        file: {
            "content": inspect.cleandoc(content),
            "init_line": f"{init_lines[file]} # noqa: F401",
        }
        for file, content in templates.items()
    }


def info(*args):
    for item in args:
        print(item, file=sys.stderr)


if __name__ == "__main__":
    main(sys.argv[1:])
