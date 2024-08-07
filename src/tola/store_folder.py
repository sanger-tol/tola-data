import logging
import re
from pathlib import Path

from ulid import ULID


class FilePattern:
    __slots__ = "is_image", "pattern", "caption"

    def __init__(self, is_image=True, pattern=None, caption=None):
        self.is_image = is_image
        self.pattern = re.compile(pattern)
        self.caption = caption

    def __repr__(self):
        return str(
            {
                "is_image": self.is_image,
                "pattern": self.pattern,
                "caption": self.caption,
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

    def add_file_pattern(self, fp: FilePattern):
        self.file_patterns.append(fp)

    def build_from_config(self, config):
        if img_pattern_list := config.get("image_file_patterns"):
            for img in img_pattern_list:
                self.add_file_pattern(FilePattern(is_image=True, **img))
        if othr_pattern_list := config.get("other_file_patterns"):
            for othr in othr_pattern_list:
                self.add_file_pattern(FilePattern(is_image=False, **othr))

    def scan_files(
        self,
        directory: Path,
        names: dict[str, str],
    ):
        patterns = [*self.file_patterns]
        max_count = len(patterns)
        count = 0
        found = {}
        size_bytes = 0
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
                        found.setdefault("image_file_list", []).append(spec)
                    else:
                        found.setdefault("other_file_list", []).append(spec)
                    break
        found["files_bytes_total"] = size_bytes

        logging.debug(f"Found {count} files out of a possible {max_count} patterns")
        if patterns:
            logging.debug(f"No files found for: {patterns!r}")

        return found


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
