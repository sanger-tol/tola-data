#!/usr/bin/env python3

import inspect
import re
import sys


def main(table_names):

    for name in table_names:
        snake, camel = snake_and_camel(name)
        templates = file_templates(snake, camel)
        for file, content in templates.items():
            print(f"{file}:\n{content}\n")


def snake_and_camel(str):
    words = [x for x in re.findall(r"([A-Z]*[a-z]*)", str) if len(x)]
    snake = "_".join(x.lower() for x in words)
    camel = "".join(x.title() for x in words)
    return snake, camel


def file_templates(snake, camel):

    # header indentation needs to match files
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

                specimen = db.relationship("Other", back_populates="{snake}")

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
    return {file: inspect.cleandoc(content) for file, content in templates.items()}


if __name__ == "__main__":
    main(sys.argv[1:])
