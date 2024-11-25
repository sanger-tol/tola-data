import datetime
import json

import click


def field_style(column_name, val):
    """Returns a stringified field and style given a column name and value"""

    if val == "":
        return "<empty_string>", bold_red
    if val is None:
        return "null", dim
    if isinstance(val, datetime.date):
        return val.isoformat(), bold
    if isinstance(val, int) and val >= 10_000 and not column_name.endswith("_id"):
        return f"{val:_}", bold
    if isinstance(val, dict | list):
        return json.dumps(val, indent=2), bold
    return repr(val), bold


def dim(txt):
    return click.style(txt, dim=True)


def bold_green(txt):
    return click.style(txt, bold=True, fg="green")


def bold_red(txt):
    return click.style(txt, bold=True, fg="red")


def bold(txt):
    return click.style(txt, bold=True)



def colour_pager(itr):
    """
    click fails to detect that output can be coloured when the script is at
    the end of a UNIX pipe and STDOUT is a tty, so we set the `color`
    parmameter here to `True` which overrides its autodetection.
    """
    click.echo_via_pager(itr, color=True)
