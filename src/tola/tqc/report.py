import json
import sys

import click
import requests

from tola.pretty import bold, s
from tola.terminal import TerminalDict


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

    Supply an optional list of `key=value` PARAMS which will be correctly URL
    encoded before sending to the server.

    e.g.

        tqc report pipeline-data 'species_id=Vulpes vulpes'

        tqc report folder/data data_id=48728_7-8#1
    """

    client = ctx.obj
    first_key, payload = build_payload(params, report_format)

    report = f"report/{report_name}"
    if show_url:
        req = requests.Request(
            "GET", client.build_path(report), params=payload
        ).prepare()
        print(req.url)
        return

    itr = client.stream_lines(report, payload)

    if report_format == "TSV":
        print_tsv(itr)
    else:
        try:
            first = next(itr)
        except StopIteration:
            # Zero lines in report
            return

        if sys.stdout.isatty():
            click.echo_via_pager(pretty_terminal_dict_itr(first, itr, first_key))
        else:
            out = sys.stdout.buffer
            out.write(first + b"\n")
            for line in itr:
                out.write(line + b"\n")


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
    yield f"\nReport has {bold(row_count)} row{s(row_count)}"


def print_tsv(itr):
    out = sys.stdout.buffer
    for row in itr:
        out.write(row + b"\n")


def build_payload(params, report_format="NDJSON"):
    param_dict = {}
    first_key = None
    for spec in params:
        k, v = spec.split("=", maxsplit=1)
        param_dict[k] = v
        if first_key is None:
            first_key = k
    param_dict["format"] = report_format
    return first_key, param_dict
