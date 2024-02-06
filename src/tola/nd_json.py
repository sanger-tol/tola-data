import datetime
import json

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()

def ndjson_row(data):
    return json.dumps(data, cls=DateTimeEncoder) + "\n"
