import json
import sys

import click

from tola.pretty import bold_red
from tola.terminal import TerminalDict


@click.command
@click.pass_context
@click.argument(
    "report-name",
    nargs=1,
    required=True,
)
@click.argument(
    "params",
    nargs=-1,
    required=False,
)
def report(ctx, report_name, params):
    """
    Fetch data from ToLQC `/report` endpoints.

    Supply an optional list of `key=value` PARAMS which will be correctly URL
    encoded before sending to the server.

    e.g.

        tqc report pipeline-data 'species_id=Vulpes vulpes'

        tqc report folder/data data_id=48728_7-8#1
    """

    client = ctx.obj
    first_key, payload = build_payload(params)
    itr = client.stream_lines(f"report/{report_name}", payload)
    first = next(itr)
    check_for_error(first)

    if first:
        if sys.stdout.isatty():
            click.echo_via_pager(pretty_terminal_dict_itr(first, itr, first_key))
        else:
            sys.stdout.buffer.write(first)
            for line in itr:
                sys.stdout.buffer.write(line)


def pretty_terminal_dict_itr(first, itr, first_key=None):
    if first:
        obj = json.loads(first)
        max_hdr = max(len(x) for x in obj)
        yield TerminalDict(obj, key=first_key, max_key_length=max_hdr).pretty()
        for line in itr:
            obj = json.loads(line)
            yield TerminalDict(obj, key=first_key, max_key_length=max_hdr).pretty()


def check_for_error(line):
    obj = json.loads(line)
    if (errors := obj.get("errors")) and isinstance(errors, list):
        detail = errors[0].get("detail")
        exit(bold_red(detail))


def build_payload(params):
    param_dict = {}
    first_key = None
    for spec in params:
        k, v = spec.split("=", maxsplit=1)
        param_dict[k] = v
        if first_key is None:
            first_key = k
    param_dict["format"] = "NDJSON"
    return first_key, param_dict
