import sys

import click

from tola.ndjson import ndjson_row
from tola.pretty import bold, colour_pager, s
from tola.terminal import (
    TerminalDict,
    TerminalDiff,
    dry_warning,
    pretty_terminal_itr,
)
from tola.tqc.engine import (
    core_data_object_to_dict,
    dicts_to_core_data_objects,
    key_list_search,
)


def upsert_rows(client, table, input_obj, apply_flag=False):
    key = f"{table}.id"

    idx_db_obj = key_list_search(client, table, key, [x[key] for x in input_obj])

    # Modification metadata is not editable
    ignore = {"modified_by", "modified_at", key}

    upserts = []
    changes = []
    diff_count = 0
    new_count = 0
    for inp in input_obj:
        oid = inp[key]
        if obj := idx_db_obj.get(oid):
            flat = core_data_object_to_dict(obj)
            attr = {}
            chng = {key: oid}
            for k, inp_v in inp.items():
                if k in ignore:
                    continue
                flat_v = flat.get(k)
                if inp_v != flat_v:
                    attr[k] = inp_v
                    chng[k] = flat_v, inp_v
            if attr:
                diff_count += 1
                changes.append(TerminalDiff(chng))
                upserts.append({key: oid, **attr})
        else:
            new_count += 1
            changes.append(TerminalDict(inp))
            upserts.append(inp)

    if upserts:
        if apply_flag:
            verb = "Made"
            action = "created "
            ads = client.ads
            for chunk in client.pages(dicts_to_core_data_objects(ads, table, upserts)):
                ads.upsert(table, chunk)
        else:
            verb = "Found"
            action = ""

        header = (
            f"{verb} {bold(diff_count)} change{s(diff_count)}"
            f" and {action}{bold(new_count)} new row{s(new_count)}:\n"
        )

        if sys.stdout.isatty():
            colour_pager(pretty_terminal_itr(changes, header, apply_flag))
        else:
            for chng in changes:
                sys.stdout.write(ndjson_row(chng.data))
            if not apply_flag:
                click.echo(dry_warning(len(upserts)), err=True)
