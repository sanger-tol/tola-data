import datetime

from click import style


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
    return repr(val), bold


def dim(txt):
    return style(txt, dim=True)


def bold_green(txt):
    return style(txt, bold=True, fg="green")


def bold_red(txt):
    return style(txt, bold=True, fg="red")


def bold(txt):
    return style(txt, bold=True)
