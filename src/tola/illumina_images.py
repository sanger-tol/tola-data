import subprocess
from pathlib import Path
from shutil import copy
from tempfile import TemporaryDirectory

from partisan.irods import DataObject

from tola.tqc.engine import irods_path_dataobject


class BamStatsImages:
    def __init__(
        self,
        stats_file: Path = None,
        dir_path: Path = None,
        image_list: [Path] = None,
    ):
        self.stats_file = stats_file
        self.dir_path = dir_path
        if not image_list:
            image_list = [p for p in dir_path.iterdir() if p.suffix == ".png"]
        self.image_list = image_list

    def parse_stats_file(self):
        reads = None
        bases = None
        stats_file_path = self.dir_path / self.stats_file
        for line in stats_file_path.open():
            fields = line.rstrip("\r\n").split("\t")
            if fields[0] == "SN":
                name = fields[1].rstrip(":")
                if name == "sequences":
                    reads = int(fields[2])
                elif name == "total length":
                    bases = int(fields[2])
            if reads is not None and bases is not None:
                break

        self.reads = reads
        self.bases = bases

    def __str__(self):
        return f"{self.stats_file}\n" + "".join([f"  {x}\n" for x in self.image_list])


class PlotBamStatsRunner:
    def run_bamstats_in_tmpdir(self, bam_file: str) -> BamStatsImages:
        self.tmp_dir = TemporaryDirectory()
        return self.run_bamstats(bam_file, run_path=Path(self.tmp_dir.name))

    def run_bamstats(self, bam_file: str, run_path: Path = Path()) -> BamStatsImages:
        bam_path, irods_obj = irods_path_dataobject(bam_file)

        # Build Paths to local and remote stats files
        stats_file = Path(bam_path.stem + "_F0xB00.stats")
        remote_path = bam_path.parent / stats_file
        local_path = run_path / stats_file

        if remote_path != local_path:
            if irods_obj:
                DataObject(remote_path).get(local_path, verify_checksum=True)
            else:
                copy(remote_path, local_path)

        subprocess.run(  # noqa: S603
            [  # noqa: S607
                "plot-bamstats",
                "-p",
                str(run_path / local_path.stem),
                str(local_path),
            ],
            capture_output=True,
            check=True,
            text=True,
        )

        return BamStatsImages(stats_file=stats_file, dir_path=run_path)
