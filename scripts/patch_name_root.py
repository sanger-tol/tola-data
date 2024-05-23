import json
import sys

from tola.fetch_mlwh_seq_data import trimmed_tag
from tola.ndjson import ndjson_row


def main():
    for line in sys.stdin:
        row = json.loads(line)
        name = row["name_root"]
        movie, *tags = name.split("#")
        tag1 = trimmed_tag(tags[0]) if len(tags) else None
        tag2 = trimmed_tag(tags[1]) if len(tags) > 1 else None
        if tag2:
            new_name = f"{movie}#{tag1}#{tag2}"
        elif tag1:
            new_name = f"{movie}#{tag1}"
        else:
            new_name = name
        if name != new_name:
            sys.stdout.write(
                ndjson_row(
                    {
                        "data_id": str(row["data_id"]),
                        "name_root": new_name,
                    }
                )
            )


if __name__ == "__main__":
    main()
