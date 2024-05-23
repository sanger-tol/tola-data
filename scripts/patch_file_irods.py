import json
import sys

from tola.ndjson import ndjson_row


def main():
    for line in sys.stdin:
        row = json.loads(line)
        rem = row["remote_path"]
        if rem.startswith("/seq/"):
            sys.stdout.write(
                ndjson_row(
                    {
                        "file_id": row["file_id"],
                        "remote_path": "irods:" + rem,
                    }
                )
            )


if __name__ == "__main__":
    main()
