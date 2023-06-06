#!/usr/bin/env python3

import datetime
import inspect
import pathlib
import re
import sys

import black
from tolqc_schema import Base


def main(table_names):
    ignore_files = {"__init__.py", "__pycache__", "environment.py"}
    root_folders = tuple(
        pathlib.Path(x) for x in ("model", "resource", "schema", "service", "swagger")
    )
    class_by_name = {
        mppr.class_.__name__: mppr.class_ for mppr in Base.registry.mappers
    }
    if not class_by_name:
        msg = "No classes found in tolqc_schema"
        raise Exception(msg)

    if not table_names:
        table_names = class_by_name.keys()

    templates = create_templates(class_by_name)

    files_to_delete(root_folders, templates, ignore_files)

    for name in table_names:
        snake, camel = snake_and_camel(name)
        create_table_files(snake, templates[snake])

    rewrite_init_files(root_folders, templates)
    fixup_api_py(ignore_files)


def create_templates(class_by_name):
    templates = {}
    for name in sorted(class_by_name.keys()):
        snake, camel = snake_and_camel(name)
        class_code = fetch_alchemy_class_code(class_by_name, camel)
        templates[snake] = file_templates(snake, camel, class_code)
    return templates


def fixup_api_py(ignore_files):
    """
    Edit the `api.py` file to include the correct list of resoure file imports and uses
    """
    resource_dir = pathlib.Path("resource")
    res_list = sorted(
        f"api_{x.stem}" for x in resource_dir.iterdir() if x.name not in ignore_files
    )
    api_py_file = pathlib.Path("route/api.py")
    api_py_text = api_py_file.read_text()
    api_py_text = re.sub(
        r"(?<=from main\.resource import).+?(?=\n\n|\bfrom\b)",
        f" {', '.join(res_list)}\n",
        api_py_text,
        flags=re.DOTALL,
    )
    api_py_text = re.sub(
        r"(    api\.add_namespace\(\w+\)\s*\n)+",
        "".join(f"    api.add_namespace({x})\n" for x in res_list) + "\n\n",
        api_py_text,
        flags=re.DOTALL,
    )
    api_py_file.write_text(clean_code(api_py_text))


def files_to_delete(root_folders, templates, ignore_files):
    """
    For files found in `model/` which are no longer in the schema, prints
    a list of `git rm` commands for it and any matching files found in
    the other root folders.
    """
    for file in pathlib.Path("model").iterdir():
        if file.name in ignore_files:
            continue
        if not templates.get(file.stem, None):
            for fldr in root_folders:
                fldr_file = fldr / file.name
                if fldr_file.exists():
                    print(f"git rm '{fldr_file}'")


def fetch_alchemy_class_code(class_by_name, name):
    cls = class_by_name.get(name, None)

    if not cls:
        msg = f"No such class {name}"
        raise Exception(msg)

    return inspect.getsource(cls)


def create_table_files(snake, tmplt):
    for root, conf in tmplt.items():
        root_dir = pathlib.Path(root)
        if not root_dir.is_dir():
            root_dir.mkdir(parents=True)
            info(f"Created directory: '{root}'")

        source = root_dir / f"{snake}.py"
        with source.open(mode="w") as source_fh:
            source_fh.writelines(conf["content"])


def rewrite_init_files(root_folders, templates):
    for fldr in root_folders:
        init = fldr / "__init__.py"
        init_content = strip_dot_imports(init.read_text()) + "\n"
        for tmpl in templates.values():
            init_content += tmpl[fldr.name]["init_line"] + "\n"
        init.write_text(clean_code(init_content))


def strip_dot_imports(text):
    """Remove lines such as:

    from .accession import api_accession # noqa: F401
    """
    return re.sub(r"^from \.(.+)\n", r"", text, flags=re.MULTILINE)


def snake_and_camel(name):
    qc_dict = "qc_dict", "QCDict"
    if name in qc_dict:
        return qc_dict

    words = [x for x in re.findall(r"([A-Z]*[a-z]*)", name) if len(x)]
    snake = "_".join(x.lower() for x in words)
    camel = "".join(x.title() for x in words)
    return snake, camel


def uglify_model_code(code):
    code_db_types = re.sub(
        r"(?<!db\.)(Boolean|DateTime|Float|Integer|JSON|String)(\(\))?",
        r"db.\1()",
        code,
    )
    return re.sub(
        r"(?<!db\.)(Column|ForeignKey|UniqueConstraint|relationship)\(",
        r"db.\1(",
        code_db_types,
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
    return ""


def indent_all_but_first_line(level, code):
    indent = "\n" + (" " * level)
    return re.sub(r"\n", indent, code)


def get_base_flavour(code):
    base_match = re.search(r"class \w+\((\w+)\)", code)
    if base_match:
        return base_match.group(1)
    else:
        msg = f"Unable to determine base class from code:\n{code}"
        raise Exception(msg)


def file_templates(snake, camel, class_code):
    this_year = datetime.date.today().year

    schema_id_attr = make_id_attribute(class_code)
    schema_imports = (
        "BaseSchema, Str, setup_schema"
        if schema_id_attr
        else "BaseSchema, setup_schema"
    )
    ugly_code = uglify_model_code(class_code)
    assn_proxy_import = (
        "from sqlalchemy.ext.associationproxy import association_proxy"
        if "association_proxy(" in class_code
        else ""
    )
    base_flavour = get_base_flavour(class_code)

    # header indentation needs to match content in templates dict
    header = f"""
            # SPDX-FileCopyrightText: {this_year} Genome Research Ltd.
            #
            # SPDX-License-Identifier: MIT
            """

    templates = {
        "model": f"""
            {header}

            from tol.api_base.model import {base_flavour}, db, setup_model
            {assn_proxy_import}


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
            "content": clean_code(single_quote_code(content)),
            "init_line": f"{init_lines[file]} # noqa: F401",
        }
        for file, content in templates.items()
    }


quote_trans = str.maketrans("\"'", "'\"")


def single_quote_code(code):
    return code.translate(quote_trans)


black_mode = black.Mode(
    string_normalization=False,
    target_versions={
        black.TargetVersion[f"PY{sys.version_info.major}{sys.version_info.minor}"],
    },
)


def clean_code(code):
    return black.format_str(inspect.cleandoc(code), mode=black_mode)


def info(*args):
    for item in args:
        print(item, file=sys.stderr)


if __name__ == "__main__":
    main(sys.argv[1:])
