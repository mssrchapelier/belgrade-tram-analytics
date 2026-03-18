from typing import List, Tuple, Any
from pathlib import Path
from textwrap import dedent

import numpy as np
from numpy import float64
from numpy.typing import NDArray

from scipy.stats import ttest_rel
from scipy.stats._result_classes import TtestResult

def paired_t_test(data_path: Path, out_path: Path) -> None:
    """
    Print descriptive stats and perform a paired t-test on data from `data_path`.
    `data_path` must be a path to a CSV file with two columns, a header,
    and the rows representing observation pairs.
    Results are written to `out_path`.
    """
    with open(data_path, "r", encoding="utf8") as fin:
        first_line: str = fin.readline().strip()
    col_names: Tuple[str, ...] = tuple(first_line.split(",")[:2])
    assert len(col_names) == 2

    data: NDArray[float64] = np.loadtxt(
        data_path, dtype=float64, delimiter=",",
        # skip the header row
        skiprows=1
    )

    col_0: NDArray[float64] = data[:, 0]
    col_1: NDArray[float64] = data[:, 1]

    result_lines: List[str] = list()
    for arr, arr_name in zip((col_0, col_1), col_names): # type: NDArray[float64], str
        line: str = f"{arr_name}: mean {arr.mean():.4f} s, std {arr.std():.4f} s per job (n = {arr.size})"
        result_lines.append(line)

    result_lines.append(dedent(
        """
        Paired t-test.
        - Null hypothesis: expected values for both samples are equal.
        - Alternative hypothesis: the means of the underlying distributions are not equal.
        """
    ))

    ttest_result: TtestResult[Any] = ttest_rel(col_0, col_1, alternative="two-sided")
    significance_level: float = 0.05
    p_value: float = ttest_result.pvalue
    is_rejected: bool = p_value <= significance_level

    result_lines.append(dedent(
        f"""\
            p = {p_value:.4f}
            Null hypothesis rejected?: {str(is_rejected).upper()}
            """
    ))

    out_path.write_text("\n".join(result_lines), encoding="utf8")
    print(f"Results written to: {out_path}")
