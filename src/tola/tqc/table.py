import sys
from io import StringIO

import click

from tola.pretty import bold, bold_green, strip_ansi
from tola.terminal import colour_pager
from tola.tolqc_client import TolClient


@click.command
@click.pass_context
@click.option(
    "--show-edit-tables",
    "-e",
    flag_value=True,
    default=False,
    show_default=True,
    help="Show the `edit_...` tables",
)
@click.argument(
    "table_names",
    metavar="[TABLE]...",
    nargs=-1,
    required=False,
)
def table(ctx, show_edit_tables, table_names):
    """
    Show ToLQC database table attributes and relationships.

    Each attribute or relationship name is followed by the type of its value.
    For relationships the type is the name of the related table.  In the case
    of to-one relationships it is preceeded by "<", and for to-many
    relationships the type is surrounded by "[]".

    Shows all of the tables when called without any TABLE arguments.
    """

    client: TolClient = ctx.obj
    ads = client.ads_ro

    attr_types = ads.attribute_types
    if table_names:
        for name in table_names:
            if not attr_types.get(name):
                sys.exit(f"No such table {name!r}")
    else:
        table_names = []
        for tbl in sorted(ads.attribute_types):
            if not show_edit_tables and tbl.startswith("edit_"):
                continue
            table_names.append(tbl)

    output = []
    for table in table_names:
        attrs = attr_types[table]
        rel_conf = ads.relationship_config.get(table)
        output.append(format_table(table, attrs, rel_conf))

    if output:
        output.append("\n")
        if sys.stdout.isatty():
            colour_pager(output)
        else:
            out_fh = sys.stdout
            for tbl in output:
                out_fh.write(strip_ansi(tbl))


def format_table(table, attributes, rel_conf):
    flds = {"id": bold(attributes.get("id"))}

    if rel_conf:
        for rel_name, tbl in rel_conf.to_one.items():
            flds[rel_name] = f"< {bold(tbl)}"

    for attr, attr_type in attributes.items():
        if attr == "id":
            continue
        flds[attr] = bold(attr_type)

    if rel_conf:
        for rel_name, tbl in rel_conf.to_many.items():
            flds[rel_name] = f"[ {bold(tbl)} ]"

    fmt = StringIO()
    fmt.write(f"\n{bold_green(table)}:\n")
    fld_max = max(len(x) for x in flds)
    for fld, val in flds.items():
        fmt.write(f"  {fld:>{fld_max}}  {val}\n")
    return fmt.getvalue()
