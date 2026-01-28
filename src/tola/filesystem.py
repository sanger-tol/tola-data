import json
from pathlib import Path

from tola.ndjson import parse_ndjson_stream


class TolFileSystemError(Exception):
    """Error reading or writing from the ToL filesystem."""


def find_file(rdir: Path, glob_pattern: str) -> Path | None:
    """
    Finds a file matching the pattern in the given directory.
    Throws a `TolFileSystemError` if more than one file is found.
    """
    found = None
    for fn in rdir.glob(glob_pattern):
        if found:
            msg = f"More than one '{glob_pattern}' in '{rdir}': '{found}' and '{fn}'"
            raise TolFileSystemError(msg)
        else:
            found = fn
    return found


def find_file_or_raise(rdir: Path, glob_pattern: str) -> Path:
    """
    Finds a file matching the pattern in the given directory.
    Throws a `TolFileSystemError` unless one and only one file is found.
    """
    if file := find_file(rdir, glob_pattern):
        return file
    msg = f"Failed to find file matching '{glob_pattern}' in '{rdir}'"
    raise TolFileSystemError(msg)


def file_json_contents(file: Path):
    """
    Loads the contents of the JSON file.
    """
    return json.loads(file.read_text())


def latest_dataset_id_or_raise(rdir: Path) -> str:
    """
    Returns the latest dataset.id from a `datasets.ndjson` file in or above
    the supplied `rdir`.
    Throws a TolFileSystemError if no dataset is found.
    """
    dataset_id = latest_dataset_id(rdir)
    if not dataset_id:
        msg = (
            "Failed to find dataset_id from a 'datasets.ndjson'"
            f" file in or above directory '{rdir}'"
        )
        raise TolFileSystemError(msg)
    return dataset_id


def latest_dataset_id(path: Path) -> str | None:
    """
    Returns the latest dataset.id from a `datasets.ndjson` file in or above
    the supplied `path` (which can be a file or a directory).
    """
    ds_dir = path if path.is_dir() else path.parent
    if (ds_file := find_dataset_file(ds_dir)) and (latest := latest_dataset(ds_file)):
        return latest["dataset.id"]
    return None


def latest_dataset(ds_file: Path) -> dict | None:
    """
    Returns the latest dataset (last row) from the `ds_file`.
    """
    latest = None
    for ds in parse_ndjson_stream(ds_file.open()):
        # `latest` will be set to the last dataset in the file
        latest = ds
    return latest


def find_dataset_file(directory: Path) -> Path | None:
    """
    Searches up the directory path for file named `datasets.ndjson`.
    """
    look = directory.absolute()
    found = None
    while not found:
        dsf = look / "datasets.ndjson"
        if dsf.exists():
            found = dsf
        elif str(look) == look.root:
            break
        else:
            look = look.parent
    return found
