from typing import List
from pathlib import Path

import numpy as np
from numpy import float64
from numpy.typing import NDArray

def compare_profiling_results_for_prettify() -> None:
    # Testing whether prettifying HTML output in
    # `v1.src.dashboard.render.render.LiveStateRenderer.render`
    # (when calling with `prettify=True`)
    # adds significant overhead.
    #
    # (It most certainly does!)
    parent_dir: Path = Path(__file__).resolve().parent
    with_prettify_results_path: Path = parent_dir / "with_prettify.txt"
    without_prettify_results_path: Path = parent_dir / "without_prettify.txt"
    out_path: Path = parent_dir / "results.txt"

    with_prettify: NDArray[float64] = np.loadtxt(
        with_prettify_results_path, dtype=float64
    )
    without_prettify: NDArray[float64] = np.loadtxt(
        without_prettify_results_path, dtype=float64
    )

    result_lines: List[str] = list()
    for arr, arr_name in zip(
            (with_prettify, without_prettify),
            ("With prettify", "Without prettify")
    ): # type: NDArray[float64], str
        line: str = f"{arr_name}: mean {arr.mean():.4f} s, std {arr.std():.4f} s per job (n = {arr.size})"
        result_lines.append(line)

    out_path.write_text("\n".join(result_lines), encoding="utf8")
    print(f"Results written to: {out_path}")

if __name__ == "__main__":
    compare_profiling_results_for_prettify()