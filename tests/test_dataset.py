import re
from pathlib import Path
from shutil import copytree
from textwrap import dedent

from click.testing import CliRunner

from tola.tqc.dataset import latest_dataset_id
from tola.tqc.tqc_cmd import cli


def test_latest_dataset_id(fofn_dir):
    assert latest_dataset_id(fofn_dir / "pacbio") == "01JJ7ETRXDJJR0HXR9E5ZM434J"


def test_fofn_dir(fofn_dir, test_alias):
    runner = CliRunner(mix_stderr=False)
    with runner.isolated_filesystem() as tmp_str:
        tmp_path = Path(tmp_str) / "fofn"
        copytree(fofn_dir, tmp_path)
        args = (
            "--tolqc-alias",
            test_alias,
            "dataset",
            "--fofn",
            str(tmp_path),
        )
        result = runner.invoke(cli, args)
        assert result.exit_code == 0
        assert re.match(
            dedent(
                r"""
                1 (new|existing) dataset:

                  [0-9A-Z]{26}
                    45850_1#1
                    m64016e_230317_181135#1010
                    m64125e_240107_042905#2041
                    m64229e_220812_123827#1003
                    m64230e_220813_174404#1009
                    m84093_240530_110314_s2#2058
                $
                """
            ).rstrip(),
            result.stderr,
        )
