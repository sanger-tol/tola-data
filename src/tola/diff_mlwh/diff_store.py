import io
import os
import sys
from typing import Any

import click
import pyarrow

from tola.pretty import bg_green, bg_red, bold, colour_pager, field_style


class Mismatch:
    """Stores a row from the diff query"""

    __slots__ = (
        "data_id",
        "mlwh",
        "tolqc",
        "mlwh_hash",
        "tolqc_hash",
        "differing_columns",
        "reasons",
    )

    def __init__(
        self,
        data_id: str,
        mlwh: dict[str, Any],
        tolqc: dict[str, Any],
        mlwh_hash: str,
        tolqc_hash: str,
        differing_columns: list[str] = None,
        reasons: list[str] = None,
    ):
        self.data_id = data_id
        self.mlwh = mlwh
        self.tolqc = tolqc
        self.mlwh_hash = mlwh_hash
        self.tolqc_hash = tolqc_hash
        self.reasons = reasons
        if differing_columns:
            self.differing_columns = differing_columns
        else:
            self._build_differing_columns()

    @property
    def diff_class(self) -> list[str]:
        return ",".join(self.differing_columns)

    def differences_dict(self, show_columns):
        dd = {
            "data_id": self.data_id,
            "sample_name": self.mlwh["sample_name"],
            "reasons": rsns if (rsns := self.reasons) else [],
        }
        col_names = set(self.differing_columns)
        if show_columns:
            col_names |= show_columns
        for col in self.mlwh:
            if col in col_names:
                dd[col] = (self.mlwh[col], self.tolqc[col])
        return dd

    def _build_differing_columns(self):
        mlwh = self.mlwh
        tolqc = self.tolqc

        diff_cols = []
        for fld in mlwh:
            if mlwh[fld] != tolqc[fld]:
                diff_cols.append(fld)
        if not diff_cols:
            msg = f"Failed to find any differing columns in:\n{mlwh = }\n{tolqc = }"
            raise ValueError(msg)

        self.differing_columns = diff_cols

    def get_patch_for_table(self, table, col_map):
        mlwh = self.mlwh
        tolqc = self.tolqc

        patch = {}
        primary_key = None
        for key, out_key in col_map.items():
            if out_key == table + ".id":
                primary_key = key
                continue
            mlwh_v = mlwh[key]
            tolqc_v = tolqc[key]
            if mlwh_v != tolqc_v:
                patch[out_key] = mlwh_v
        if not primary_key:
            msg = f"Failed to find primary key in: {col_map}"
            raise ValueError(msg)
        if patch:
            pk_out = col_map[primary_key]
            patch[pk_out] = mlwh[primary_key]
            return patch
        return None

    def pretty(self, show_columns=None):
        fmt = io.StringIO()
        fmt.write(f"\n{bold(self.data_id)}")
        if sn := self.mlwh.get("sample_name"):
            fmt.write(f"  {sn}")
        if self.reasons:
            fmt.write(f"  ({' & '.join(bold(x) for x in self.reasons)})")
        fmt.write("\n")

        diff_set = set(self.differing_columns)

        # Build a set of column names which will be shown
        if show_columns:
            col_names = []
            if "ALL" in show_columns:
                for col, mlwh_v in self.mlwh.items():
                    # Skip columns where both values are None
                    # Avoids showing all the empty PacBio columns for Illumina
                    if mlwh_v is not None or self.tolqc[col] is not None:
                        col_names.append(col)
            else:
                show_set = diff_set | show_columns
                for col in self.mlwh:
                    if col in show_set:
                        col_names.append(col)
        else:
            col_names = self.differing_columns

        # Record column and value widths and style each value
        max_col_width = 0
        max_val_width = 0
        mlwh_values = []
        tolqc_values = []
        for col in col_names:
            mlwh_v, mlwh_style = field_style(col, self.mlwh[col])
            mlwh_values.append((mlwh_v, mlwh_style))

            tolqc_v, tolqc_style = field_style(col, self.tolqc[col])
            tolqc_values.append((tolqc_v, tolqc_style))

            max_col_width = max(max_col_width, len(col))
            max_val_width = max(max_val_width, len(mlwh_v), len(tolqc_v))

        max_val_width = self.fit_max_data_width_to_terminal(
            max_col_width, max_val_width
        )

        # Create the pretty output
        fmt.write(f"  {'':{max_col_width}}  {'MLWH':{max_val_width}}  ToLQC\n")
        for col, (mlwh_val, mlwh_style), (tolqc_val, tolqc_style) in zip(
            col_names,
            mlwh_values,
            tolqc_values,
            strict=True,
        ):
            for mlwh_v, tolqc_v in self.wrap_values(max_val_width, mlwh_val, tolqc_val):
                pad = " " * (max_val_width - len(mlwh_v))
                mlwh_fmt = mlwh_style(mlwh_v)
                tolqc_fmt = tolqc_style(tolqc_v)
                if show_columns:
                    # When extra columns have been requested, highlight matching
                    # and differing values.
                    if mlwh_v == tolqc_v:
                        mlwh_fmt = bg_green(mlwh_fmt)
                        tolqc_fmt = bg_green(tolqc_fmt)
                    else:
                        if mlwh_v != "null":
                            mlwh_fmt = bg_red(mlwh_fmt)
                        if tolqc_v != "null":
                            tolqc_fmt = bg_red(tolqc_fmt)

                fmt.write(f"  {col:>{max_col_width}}  {mlwh_fmt}{pad}  {tolqc_fmt}\n")
                col = "..."

        return fmt.getvalue()

    @staticmethod
    def fit_max_data_width_to_terminal(max_col_width, max_data_width):
        if sys.stdout.isatty():
            term_width, _ = os.get_terminal_size()
        else:
            term_width = 132
        return min(max_data_width, (term_width - max_col_width - 6) // 2)

    @staticmethod
    def wrap_values(max_width, frst, scnd):
        max_v_len = max(len(frst), len(scnd))
        for i in range(0, max_v_len, max_width):
            end = i + max_width
            yield frst[i:end], scnd[i:end]


class DiffStore:
    """Accumulates diff results"""

    def __init__(self):
        self.data_id = []
        self.mlwh_hash = []
        self.tolqc_hash = []
        self.differing_columns = []

    def add(self, m: Mismatch):
        self.data_id.append(m.data_id)
        self.mlwh_hash.append(m.mlwh_hash)
        self.tolqc_hash.append(m.tolqc_hash)
        self.differing_columns.append(m.differing_columns)

    def arrow_table(self):
        return pyarrow.Table.from_pydict(
            {
                "data_id": pyarrow.array(self.data_id),
                "mlwh_hash": pyarrow.array(self.mlwh_hash),
                "tolqc_hash": pyarrow.array(self.tolqc_hash),
                "differing_columns": pyarrow.array(self.differing_columns),
            }
        )


def write_pretty_output(
    diffs: list[Mismatch], show_columns=None, filehandle=sys.stdout
):
    if filehandle.isatty():
        colour_pager(pretty_diff_iterator(diffs, show_columns))
    else:
        # Prevent empty emails being sent from cron jobs.
        # echo_via_pager() prints a newline if there are no diffs to
        # print, so avoid it if not attached to a TTY.
        for txt in pretty_diff_iterator(diffs, show_columns):
            click.echo(txt)


def pretty_diff_iterator(itr: list[Mismatch], show_columns=None):
    n = 0
    for m in itr:
        n += 1
        yield m.pretty(show_columns)

    if n:
        yield f"\n{bold(n)} mismatch{'es' if n > 1 else ''} between MLWH and ToLQC"
