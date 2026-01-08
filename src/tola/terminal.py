import io
import json
import os
import subprocess
import sys

from tola.pretty import bold, bold_green, field_style, s
from tola.tqc.engine import cdo_type_id, core_data_object_to_dict


class TerminalObj:
    __slots__ = "data", "key", "max_key_length"

    def __init__(self, data, key=None, max_key_length=None):
        self.data = data
        self.key = key
        self.max_key_length = max_key_length

    def _max_hdr(self):
        return self.max_key_length or max(len(x) for x in self.data)


class TerminalDict(TerminalObj):
    def pretty(self):
        max_hdr = self._max_hdr()
        key = self.key

        fmt = io.StringIO()
        fmt.write("\n")
        for k, v in self.data.items():
            v, style = field_style(k, v)
            if k == key:
                style = bold_green

            first, *rest = v.splitlines()
            fmt.write(f" {k:>{max_hdr}}  {style(first)}\n")
            for r in rest:
                fmt.write(f" {'':{max_hdr}}  {style(r)}\n")

        return fmt.getvalue()


class TerminalDiff(TerminalObj):
    def pretty(self):
        max_hdr = self._max_hdr()
        key, *v_keys = chng = self.data

        old_values = []
        new_values = []
        for k in v_keys:
            old, new = chng[k]
            old_values.append(field_style(k, old))
            new_values.append(field_style(k, new))

        fmt = io.StringIO()
        hdr, hdr_style = field_style(key, chng[key])
        fmt.write(f"\n {key:>{max_hdr}}  {hdr_style(hdr)}\n")

        old_val_max = max(len(x[0]) for x in old_values)
        for k, (old_val, old_style), (new_val, new_style) in zip(
            v_keys, old_values, new_values, strict=True
        ):
            pad = " " * (old_val_max - len(old_val))
            old_fmt = old_style(old_val)
            new_fmt = new_style(new_val)
            fmt.write(f" {k:>{max_hdr}}  {pad}{old_fmt} to {new_fmt}\n")

        return fmt.getvalue()


def pretty_cdo_itr(cdo_list, key, head=None, tail=None, show_modified=False):
    if not cdo_list:
        return []

    cdo_key = cdo_type_id(cdo_list[0])
    flat_list = [
        core_data_object_to_dict(x, show_modified=show_modified) for x in cdo_list
    ]
    return pretty_dict_itr(flat_list, key, cdo_key, head, tail)


def pretty_dict_itr(row_list, key, alt_key=None, head=None, tail=None):
    if not row_list:
        return []

    if not head:
        head = "Found {} row{}:"

    first = row_list[0]
    if key not in first and key == "id":
        if alt_key in first:
            key = alt_key
        else:
            sys.exit(
                f"Possible key values '{key}' or '{alt_key}' not found in first row:\n"
                + json.dumps(first, indent=4)
            )

    count = len(row_list)
    yield head.format(bold(count), s(count)) + "\n"

    for flat in row_list:
        yield TerminalDict(flat, key=key).pretty()

    if tail:
        count = len(row_list)
        yield "\n" + tail.format(bold(count), s(count))


def pretty_dict(flat, max_hdr, key=None):
    fmt = io.StringIO()
    fmt.write("\n")
    for k, v in flat.items():
        v, style = field_style(k, v)
        if k == key:
            style = bold_green

        first, *rest = v.splitlines()
        fmt.write(f" {k:>{max_hdr}}  {style(first)}\n")
        for r in rest:
            fmt.write(f" {'':{max_hdr}}  {style(r)}\n")
    return fmt.getvalue()


def pretty_changes_itr(changes, apply_flag):
    n_changes = len(changes)
    verb = "Made" if apply_flag else "Found"
    yield f"{verb} {bold(n_changes)} change{s(n_changes)}:\n"

    for chng in changes:
        yield TerminalDiff(chng).pretty()

    if not apply_flag:
        yield "\n" + dry_warning(len(changes))


def pretty_terminal_itr(terminal_objects, header, apply_flag):
    yield header

    for term in terminal_objects:
        yield term.pretty()

    if not apply_flag:
        yield "\n" + dry_warning(len(terminal_objects))


def dry_warning(count):
    return (
        f"Dry run. Use '--apply' flag to store {bold(count)} changed row{s(count)}.\n"
    )




def open_pager():
    pager_cmd = [os.environ.get("PAGER", "less").strip()]
    if pager_cmd[0] == "less" and not os.environ.get("LESS"):
        pager_cmd.extend(
            [
                "--no-init",
                "--quit-if-one-screen",
                "--ignore-case",
                "--RAW-CONTROL-CHARS",
            ]
        )

    return subprocess.Popen(  # noqa: S602, S603
        pager_cmd,
        stdin=subprocess.PIPE,
        text=True,
    )


def colour_pager(itr):
    if isinstance(itr, str):
        itr = [itr]

    pager = open_pager()
    try:
        for text in itr:
            pager.stdin.write(text)
    except (OSError, KeyboardInterrupt):
        pass
    pager.stdin.close()

    while True:
        try:
            pager.wait()
        except KeyboardInterrupt:
            pass
        else:
            break
