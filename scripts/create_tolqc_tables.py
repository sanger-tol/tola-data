#!/usr/bin/env python3

import black
import datetime
import inspect
import pathlib
import re
import sys

from tolqc_schema import Base


def main(table_names):
    for name in table_names:
        snake, camel = snake_and_camel(name)
        class_code = fetch_alchemy_class_code(camel)
        templates = file_templates(snake, camel, class_code)
        for root, conf in templates.items():
            create_files(root, snake, conf)


def fetch_alchemy_class_code(name):
    cls = None
    for mppr in Base.registry.mappers:
        if mppr.class_.__name__ == name:
            cls = mppr.class_
            break

    if not cls:
        raise Exception(f"No such class {name}")

    return inspect.getsource(cls)


def create_files(root, snake, conf):
    root_dir = pathlib.Path(root)
    if not root_dir.is_dir():
        root_dir.mkdir()
        info(f"Created directory: '{root}'")

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


def uglify_model_code(code):
    code_db_types = re.sub(
        r"(?<!db\.)(Boolean|DateTime|Float|Integer|String)(\(\))?", r"db.\1()", code
    )
    return re.sub(
        r"(?<!db\.)(Column|ForeignKey|relationship)\(", r"db.\1(", code_db_types
    )


def make_id_attribute(code):
    primary_key_match = re.search(
        r"""
            (\w+)\s*=\s*          # e.g. "accession_id = "
            (db\.)?Column\(\s*    # "Column(" optionally preceeded by "db."
            (db\.)?(\w+)\(?\d*\)? # e.g. "String" or "db.String()" or "db.String(32)"
            [\w=,\s\[\]]*         # Keyword arguments, e.g. "foreign_keys=[species_id]"
            primary_key=True
            [\w=,\s\[\]]*         # Keyword arguments
            \)                    # Closing parenthesis of "Column(..."
        """,
        code,
        flags=re.VERBOSE,
    )
    if primary_key_match:
        id_name = primary_key_match.group(1)
        return (
            ""
            if id_name == "id"
            else f'id = Str(attribute="{id_name}", dump_only=True)  # noqa: A003'
        )
    else:
        return ""


def indent_all_but_first_line(level, code):
    indent = "\n" + (" " * level)
    return re.sub(r"\n", indent, code)


def file_templates(snake, camel, class_code):
    this_year = datetime.date.today().year

    schema_id_attr = make_id_attribute(class_code)
    schema_imports = (
        "BaseSchema, Str, setup_schema"
        if schema_id_attr
        else "BaseSchema, setup_schema"
    )
    ugly_code = uglify_model_code(class_code)

    # header indentation needs to match content in templates dict
    header = f"""
            # SPDX-FileCopyrightText: {this_year} Genome Research Ltd.
            #
            # SPDX-License-Identifier: MIT
            """

    templates = {
        "model": f"""
            {header}

            from tol.api_base.model import LogBase, db, setup_model


            @setup_model
            {indent_all_but_first_line(12, ugly_code)}

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
            from tol.api_base.schema import {schema_imports}


            @setup_schema
            class {camel}Schema(BaseSchema):
                class Meta(BaseSchema.BaseMeta):
                    model = {camel}
                {schema_id_attr}
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
            "content": clean_code(content),
            "init_line": f"{init_lines[file]} # noqa: F401",
        }
        for file, content in templates.items()
    }


black_mode = black.Mode(
    target_versions={
        black.TargetVersion[f"PY{sys.version_info.major}{sys.version_info.minor}"]
    }
)


def clean_code(code):
    return black.format_str(inspect.cleandoc(code), mode=black_mode)


def info(*args):
    for item in args:
        print(item, file=sys.stderr)


if __name__ == "__main__":
    main(sys.argv[1:])
