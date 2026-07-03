import json
import sys

import click

from tola.pretty import bold, s, wrap_name_description
from tola.terminal import TerminalDict, colour_pager


@click.command
@click.pass_context
@click.argument(
    "report-name",
    nargs=1,
    required=True,
)
@click.option(
    "--url",
    "show_url",
    is_flag=True,
    flag_value=True,
    help="Prints the URL for the report and exits.",
)
@click.option(
    "--format",
    "report_format",
    type=click.Choice(
        ["NDJSON", "TSV"],
        case_sensitive=False,
    ),
    default="NDJSON",
    show_default=True,
    help="Output format of report.",
)
@click.argument(
    "params",
    nargs=-1,
    required=False,
)
def report(ctx, show_url, report_format, report_name, params):
    """
    Fetch data from ToLQC `/report` endpoints.

    To see a list of available reports do:

        tqc report list

    Supply an optional list of `key=value` PARAMS which will be correctly URL
    encoded before sending to the server.

    \b
    e.g.
        tqc report pipeline-data species='Vulpes vulpes'
        tqc report folder/data data_id=48728_7-8#1
    """

    client = ctx.obj
    first_key, payload = build_payload(params, report_format)

    if report_name == "list":
        show_report_description(client)
        return

    if show_url:
        print(client.report_url(report_name, params=payload))
        return

    itr = client.stream_lines(f"report/{report_name}", payload)

    if report_format == "TSV":
        print_tsv(itr)
    else:
        try:
            first = next(itr)
        except StopIteration:
            # Zero lines in report
            return

        if sys.stdout.isatty():
            colour_pager(pretty_terminal_dict_itr(first, itr, first_key))
        else:
            out = sys.stdout.buffer
            out.write(first + b"\n")
            for line in itr:
                out.write(line + b"\n")


def show_report_description(client):
    meta_id = "tqc.report.list"
    (meta,) = client.ads.get_by_ids(
        "metadata", [meta_id], requested_fields=["json_value"]
    )
    rep_list = None
    if meta:
        rep_list = meta.json_value
    else:
        sys.exit(f"Error: missing metadata table entry for '{meta_id}'")

    name_desc = {}
    for avail in rep_list:
        name = avail.get("name", "<NO_NAME>")
        desc = avail.get("description", "<NO_DESCRIPTION>")
        name_desc[name] = desc

    click.echo("Available reports:")
    click.echo(wrap_name_description(name_desc))


def pretty_terminal_dict_itr(first, itr, first_key=None):
    row_count = 0
    if first:
        row_count = 1
        obj = json.loads(first)
        max_hdr = max(len(x) for x in obj)
        yield TerminalDict(obj, key=first_key, max_key_length=max_hdr).pretty()
        for line in itr:
            row_count += 1
            obj = json.loads(line)
            yield TerminalDict(obj, key=first_key, max_key_length=max_hdr).pretty()
    yield f"\nReport has {bold(row_count)} row{s(row_count)}\n"


def print_tsv(itr):
    out = sys.stdout.buffer
    for row in itr:
        out.write(row + b"\n")


def build_payload(params, report_format="NDJSON"):
    param_dict = {}
    first_key = None
    for spec in params:
        if "=" in spec:
            k, v = spec.split("=", maxsplit=1)
        else:
            k = spec
            v = True
        param_dict[k] = v
        if first_key is None:
            first_key = k
    param_dict["format"] = report_format
    return first_key, param_dict
