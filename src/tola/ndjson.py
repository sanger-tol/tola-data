import datetime
import json
import pytz
import sys

# Ideally the timezone could be collected from the source, such as the MLWH
# MySQL server.
DEFAULT_TZ = pytz.timezone("Europe/London")


class DateTimeEncoder(json.JSONEncoder):
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


def ndjson_row(data):
    return json.dumps(data, cls=DateTimeEncoder, separators=(",", ":")) + "\n"
