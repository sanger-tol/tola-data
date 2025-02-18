#!/usr/bin/env python3

import re
import sys
from subprocess import CalledProcessError, run

import tomllib


def main():
    pyproj = "pyproject.toml"
    with open(pyproj, "rb") as f:
        data = tomllib.load(f)
    version = data["project"]["version"]
    gtv = git_tag_version()
    if gtv:
        if gtv == version:
            sys.exit(f"Tag for version {version} already exists")
        elif natural(version) < natural(gtv):
            sys.exit(f"Version {version} in {pyproj} is less than latest git tag {gtv}")
    run(("git", "tag", version), check=True)  # noqa: S603
    run(("git", "push", "--tags"), check=True)  # noqa: S603


def git_tag_version():
    run(("git", "pull", "--tags"), check=True)  # noqa: S603
    git_tag = run(("git", "tag"), capture_output=True, text=True, check=True)  # noqa: S603, S607
    tags = sorted(git_tag.stdout.splitlines(), key=natural, reverse=True)
    return tags[0] if tags else None


def natural(string):
    return tuple(
        int(x) if i % 2 else x for i, x in enumerate(re.split(r"(\d+)", string))
    )


if __name__ == "__main__":
    try:
        main()
    except CalledProcessError as cpe:
        sys.exit(cpe.stderr)
