import datetime
import json
import os
import sys

import pytz

# Ideally the timezone could be collected from the source, such as the MLWH
# MySQL server.
DEFAULT_TZ = pytz.timezone(os.getenv("TZ", "Europe/London"))


class DateTimeZoneEncoder(json.JSONEncoder):
    """
    Encode `date` and `datetime` objects in ISO 8601 format, and add the
    default, local timezone to any "naive" datetime objects before encoding.
    """

    def default(self, obj):
        # Test for datetime first, since it is a subclass of date
        if isinstance(obj, datetime.datetime):
            # Is the datetime timezone "aware"?
            if not obj.tzinfo or obj.tzinfo.utcoffset(obj) is None:
                # No, datetime is "naive"
                obj = DEFAULT_TZ.localize(obj)
            return obj.isoformat()
        if isinstance(obj, datetime.date):
            return obj.isoformat()

        # This line means any exceptions raised will come from the base class
        return json.JSONEncoder.default(self, obj)


def ndjson_row(data):
    return json.dumps(data, cls=DateTimeZoneEncoder, separators=(",", ":")) + "\n"


def pretty_row(data):
    return json.dumps(data, cls=DateTimeZoneEncoder, indent=4)


def parse_ndjson_stream(stream):
    for line in stream:
        yield parse_ndjson_row(line)


def parse_ndjson_row(line):
    if len(line) > 100_000:
        # Don't get hung up parsing excessively large strings
        msg = f"Unexpectedly long line ({len(line):_d} characters) in input"
        raise ValueError(msg)
    row = json.loads(line)
    if type(row) is not dict:  # noqa: E721
        msg = f"JSON must decode to a dict, not a {type(row)}"
        raise ValueError(msg)
    for k, v in row.items():
        if type(v) is str:  # noqa: E721
            # Avoid empty strings
            stripped = v.strip()
            row[k] = None if stripped == "" else stripped
    return row


def get_input_objects(input_files):
    input_obj = []
    if input_files:
        for file in input_files:
            with file.open() as fh:
                input_obj.extend(parse_ndjson_stream(fh))
    else:
        input_obj.extend(parse_ndjson_stream(sys.stdin))

    return input_obj
