import logging
import re
from pathlib import Path

from ulid import ULID

log = logging.getLogger(__name__)


class FilePattern:
    __slots__ = "is_image", "pattern", "caption", "index"

    def __init__(self, is_image=True, pattern=None, caption=None, index=None):
        self.is_image = is_image
        self.pattern = re.compile(pattern)
        self.caption = caption
        self.index = None if index is None else int(index)

    def __repr__(self):
        return str(
            {
                "is_image": self.is_image,
                "pattern": self.pattern,
                "caption": self.caption,
                "index": self.index,
            }
        )

    def matches(self, file: Path):
        return bool(re.fullmatch(self.pattern, file.name))


class FilePatternSet:
    __slots__ = "file_patterns"

    def __init__(
        self,
        file_patterns: list[FilePattern] = None,
        config=None,
    ):
        self.file_patterns = file_patterns or []
        if config:
            self.build_from_config(config)

        # Give each pattern an index to sort on if not present, and check for
        # clashes.
        idx_seen = {}
        for i, pat in enumerate(self.file_patterns):
            idx = pat.index
            if idx is None:
                idx = pat.index = i
            if idx < 0:
                msg = f"Index {idx} for {pat!r} must not be negative"
                raise ValueError(msg)
            if other := idx_seen.get(idx):
                msg = (
                    f"Index {idx} for {pat!r} clashes with {other!r}\n"
                    'Provide a unique "index" for each file pattern in the config to'
                    " define a specific order, or for none of them to get the order"
                    " in which they are specified."
                )
                raise ValueError(msg)
            idx_seen[idx] = pat

    def add_file_pattern(self, fp: FilePattern):
        self.file_patterns.append(fp)

    def build_from_config(self, config):
        if image_pattern_list := config.get("image_file_patterns"):
            for image in image_pattern_list:
                self.add_file_pattern(FilePattern(is_image=True, **image))
        if other_pattern_list := config.get("other_file_patterns"):
            for other in other_pattern_list:
                self.add_file_pattern(FilePattern(is_image=False, **other))

    def scan_files(
        self,
        directory: Path,
        names: dict[str, str],
    ):
        patterns = [*self.file_patterns]
        max_count = len(patterns)
        count = 0
        size_bytes = 0
        image_list = {}
        other_list = {}
        for file in directory.iterdir():
            if not file.is_file():
                continue
            for i, fp in enumerate(patterns):
                if fp.matches(file):
                    count += 1
                    size_bytes += file.stat().st_size
                    patterns.pop(i)

                    # Provide a useful error message if formatting the caption fails
                    try:
                        caption = fp.caption.format(**names)
                    except KeyError as ke:
                        msg = (
                            f"Name for {ke.args[0]!r} is missing."
                            f" Names provided were: {names!r}"
                        )
                        raise ValueError(msg) from None

                    spec = {"file": file.name, "caption": caption}
                    if fp.is_image:
                        image_list[fp.index] = spec
                    else:
                        other_list[fp.index] = spec
                    break

        found = {}
        if image_list:
            found["image_file_list"] = [image_list[i] for i in sorted(image_list)]
        if other_list:
            found["other_file_list"] = [other_list[i] for i in sorted(other_list)]
        found["files_total_bytes"] = size_bytes

        log.debug(f"Found {count} files out of a possible {max_count} patterns")
        if patterns:
            log.debug(f"No files found for: {patterns!r}")

        return found if count else None


class FolderLocation:
    """Root folder for upload of data files"""

    __slots__ = "folder_location_id", "s3_bucket", "prefix", "pattern_set"

    def __init__(self, folder_location_id, uri_prefix, files_template):
        self.folder_location_id = folder_location_id
        self.parse_s3_uri(uri_prefix)
        self.pattern_set = FilePatternSet(config=files_template)

    def parse_s3_uri(self, uri_prefix):
        if m := re.match(r"s3://([^/]+)/(.+)", uri_prefix):
            self.s3_bucket = m.group(1)
            self.prefix = m.group(2)

    def list_files(self, folder):
        file_list = []
        ulid = folder.id
        for attr_key in ("image_file_list", "other_file_list"):
            if spec_list := getattr(folder, attr_key):
                for spec in spec_list:
                    file_list.append(
                        "/".join(
                            (
                                self.prefix,
                                ulid,
                                spec["file"],
                            )
                        )
                    )

        return file_list


def upload_files(
    client,
    folder_location_id: str = None,
    table: str = None,
    spec: dict = None,
):
    if dir_str := spec.get("directory"):
        directory = Path(dir_str)
    else:
        msg = "Missing 'directory' field"
        raise ValueError(msg)
    if not directory.is_dir():
        msg = f"Not a directory: '{directory}'"
        raise ValueError(msg)

    fldr_loc = client.get_folder_location(folder_location_id)
    if not fldr_loc:
        msg = f"No such FolderLocation {folder_location_id!r}"
        raise ValueError(msg)

    id_key = table + ".id"
    oid = spec.get(id_key)
    if not oid:
        msg = f"Missing expected value for '{id_key}' in row"
        raise ValueError(msg)
    (entry,) = client.ads.get_by_ids(table, [oid])
    if not entry:
        msg = f"Failed to fetch {table} with {id_key} = {oid!r}"
        raise ValueError(msg)

    # Find files and upload to S3
    ulid = None
    if files := fldr_loc.pattern_set.scan_files(directory, spec):
        ulid = str(ULID())
        for key, file_list in files.items():
            if not key.endswith("_file_list"):
                continue
            for fs in file_list:
                file = fs["file"]
                local = directory / file
                remote = "/".join((fldr_loc.prefix, ulid, file))
                log.info(f"Uploading: {local}\nTo: {remote}")
                client.s3.put_file(local.open("rb"), fldr_loc.s3_bucket, remote)

        # Store and link new Folder
        cdo = client.build_cdo
        fldr = cdo(
            "folder",
            ulid,
            {
                "folder_location_id": folder_location_id,
                **files,
            },
        )
        client.ads.upsert("folder", [fldr])
        client.ads.upsert(table, [cdo(table, oid, {"folder_ulid": ulid})])

    # Delete S3 files in old Folder
    if old_fldr := entry.folder:
        if ulid and old_fldr.id == ulid:
            msg = (
                f"Error: Folder attached to existing {table} {oid}",
                f" matches the new ULID {ulid}",
            )
            raise ValueError(msg)
        file_list = fldr_loc.list_files(old_fldr)
        client.s3.delete_files(fldr_loc.s3_bucket, file_list)

        # Delete old Folder from ToLQC
        client.ads.delete("folder", [old_fldr.id])

    return {id_key: oid, "folder.id": ulid, **files} if files else None
