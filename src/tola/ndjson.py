import datetime
import json
import os
import pytz
import sys

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
