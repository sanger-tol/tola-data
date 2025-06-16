import datetime
import json
import os
import re
import subprocess
from io import StringIO

import click


def strip_ansi(txt):
    """
    Strips any ANSI escape codes from `txt`
    """
    return re.sub(r"\033\[[;?0-9]*[a-zA-Z]", "", txt)


def plain_text_from_itr(itr):
    out = StringIO("")
    for txt in itr:
        out.write(strip_ansi(txt))

    return out.getvalue()


def field_style(column_name, val):
    """Returns a stringified field and style given a column name and value"""

    if val == "":
        return "<empty_string>", bold_red
    if val is None:
        return "null", dim
    if isinstance(val, datetime.datetime):
        return val.isoformat(sep=" ", timespec="seconds"), bold
    if isinstance(val, datetime.date | datetime.time):
        return val.isoformat(), bold
    if isinstance(val, int) and val >= 10_000 and not column_name.endswith("_id"):
        return f"{val:_}", bold
    if isinstance(val, dict | list):
        if len(val) == 0 or (
            len(val) == 1
            and isinstance(val, list)
            and not isinstance(val[0], dict | list)
        ):
            return json.dumps(val), bold
        else:
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


def bg_green(txt):
    return click.style(txt, fg="black", bg=193)


def bg_red(txt):
    return click.style(txt, fg="black", bg=224)


def s(x):
    """Formatting plurals. Argument can be an `int` or an iterable"""
    n = x if isinstance(x, int) else len(x)
    return "" if n == 1 else "s"


def colour_pager(itr):
    if isinstance(itr, str):
        itr = [itr]

    pager_cmd = [os.environ.get("PAGER", "less").strip()]
    if pager_cmd == "less" and not os.environ.get("LESS"):
        pager_cmd.extend(
            [
                "--no-init",
                "--quit-if-one-screen",
                "--ignore-case",
                "--RAW-CONTROL-CHARS",
            ]
        )

    c = subprocess.Popen(  # noqa: S602, S603
        pager_cmd,
        stdin=subprocess.PIPE,
        text=True,
    )
    try:
        for text in itr:
            c.stdin.write(text)
    except (OSError, KeyboardInterrupt):
        pass
    c.stdin.close()

    while True:
        try:
            c.wait()
        except KeyboardInterrupt:
            pass
        else:
            break


def natural(string):
    """
    Separates strings into runs of integers and strings so that they
    sort "naturally".
    """
    return tuple(
        int(x) if i % 2 else x for i, x in enumerate(re.split(r"(\d+)", string))
    )
