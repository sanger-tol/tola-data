import sys

import click

from tola import click_options
from tola.ndjson import ndjson_row
from tola.pretty import bold, colour_pager, plain_text_from_itr, s
from tola.terminal import (
    TerminalDict,
    TerminalDiff,
    dry_warning,
    pretty_terminal_itr,
)
from tola.tqc.engine import (
    core_data_object_to_dict,
    dicts_to_core_data_objects,
    input_objects_or_exit,
    key_list_search,
)


@click.command()
@click.pass_context
@click_options.table
@click_options.key
@click_options.apply_flag
@click_options.input_files
def upsert(ctx, table, key, apply_flag, input_files):
    """Add new rows or update existing rows in a table from ND-JSON input

    INPUT_FILES is a list of files in ND-JSON format.

    Specify a `--key` argument for the name of the field to use to find
    existing rows if not using the default of a primary key field named
    `<table>.id`
    """

    if key == "id":
        key = f"{table}.id"

    input_obj = input_objects_or_exit(ctx, input_files)
    ups = TableUpserter(ctx.obj)
    ups.build_table_upserts(table, input_obj, key)
    if apply_flag:
        ups.apply_upserts()
    ups.page_results(apply_flag)


class TableUpserter:
    """
    Manages queuing and reporting updates and inserts for one or more tables.
    """

    def __init__(self, client):
        self.client = client
        self.table_upserts = {}
        self.changes = []
        self.new_count = 0
        self.diff_count = 0

    def build_table_upserts(self, table, input_obj, key=None):
        """
        Compares database records to input data for a table and prepares a
        list of changes to be made.
        """

        if not key:
            key = f"{table}.id"

        idx_db_obj = key_list_search(
            self.client, table, key, [x[key] for x in input_obj]
        )

        # Modification metadata is not editable
        ignore = {"modified_by", "modified_at", key}

        upserts = []
        changes = []
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
                    self.diff_count += 1
                    changes.append(TerminalDiff(chng))
                    upserts.append({key: oid, **attr})
            else:
                self.new_count += 1
                changes.append(TerminalDict(inp))
                upserts.append(inp)

        self.table_upserts.setdefault(table, []).extend(upserts)
        self.changes.extend(changes)

    def apply_upserts(self):
        """
        Applies changes accumulated in the object across all tables listed.
        """

        client = self.client
        ads = client.ads

        output_cdo = []
        for table in list(self.table_upserts):
            upserts = self.table_upserts.pop(table)
            for chunk in client.pages(dicts_to_core_data_objects(ads, table, upserts)):
                output_cdo.extend(ads.upsert(table, chunk))
        return output_cdo

    def page_results(self, apply_flag=False, plain_text=False):
        """
        Reports formatted results to the terminal, or ND-JSON if STDOUT is not
        a TTY.
        """

        diff_count = self.diff_count
        new_count = self.new_count
        changes = self.changes

        if apply_flag:
            verb = "Made"
            action = "created "
        else:
            verb = "Found"
            action = ""
        header = (
            f"{verb} {bold(diff_count)} change{s(diff_count)}"
            f" and {action}{bold(new_count)} new row{s(new_count)}:\n"
        )

        if plain_text or sys.stdout.isatty():
            itr = pretty_terminal_itr(changes, header, apply_flag)
            if plain_text:
                return plain_text_from_itr(itr)
            else:
                colour_pager(itr)
        else:
            for chng in changes:
                sys.stdout.write(ndjson_row(chng.data))
            if not apply_flag:
                click.echo(dry_warning(len(changes)), err=True)
