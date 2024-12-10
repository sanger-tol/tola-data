import sys
from urllib.parse import quote

import click

from tola import click_options
from tola.ndjson import ndjson_row
from tola.pretty import bold, colour_pager, s
from tola.tqc.engine import (
    core_data_object_to_dict,
    dry_warning,
    fetch_list_or_exit,
    id_iterator,
    pretty_cdo_itr,
)


@click.command()
@click.pass_context
@click_options.table
@click_options.apply_flag
@click_options.file
@click_options.file_format
@click_options.id_list
def delete(ctx, table, apply_flag, file_list, file_format, id_list):
    """Delete rows from a table which match ND-JSON input lines

    The list of IDs provided must be the primary key of the table. If
    specified in files each row must contain a value for `TABLE_NAME.id`
    """

    key = f"{table}.id"
    client = ctx.obj
    ads = client.ads

    id_list = tuple(id_iterator(key, id_list, file_list, file_format))
    db_obj = fetch_list_or_exit(client, table, key, id_list)

    if db_obj:
        head = None
        tail = None
        if apply_flag:
            head = "Deleted {} row{}:"

            # Can remove call to `quote()` when ApiDataSource is fixed to
            # correctly escape IDs
            for chunk in client.pages([quote(x) for x in id_list]):
                ads.delete(table, chunk)
        else:
            tail = "Dry run. Use '--apply' flag to delete {} row{}.\n"

        if sys.stdout.isatty():
            colour_pager(pretty_cdo_itr(db_obj, key, head=head, tail=tail))
        else:
            for dlt in db_obj:
                sys.stdout.write(ndjson_row(core_data_object_to_dict(dlt)))
            if not apply_flag:
                count = len(db_obj)
                dry_warning(tail.format(bold(count), s(count)))
