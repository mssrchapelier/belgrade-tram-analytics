from typing import List, override
import statistics

from numpy import float64
from numpy.typing import NDArray

from tqdm.std import tqdm

from common.utils.dynamic_median_updater import MedianUpdaterProto
from common.utils.numpy_utils import generate_random

class GroundTruthMedianFinder(MedianUpdaterProto):

    def __init__(self):
        self._values: List[float] = []
        self._median: float | None = None

    @override
    @property
    def value(self) -> float | None:
        return self._median

    @override
    def update(self, value: float) -> float:
        self._values.append(value)
        median: float = statistics.median(self._values)
        self._median = median
        return median

class DifferentPropertyValuesException(Exception):
    pass

def _test_property_access(ground_truth: MedianUpdaterProto, tested: MedianUpdaterProto) -> None:
    value_gt: float | None = ground_truth.value
    value_tested: float | None = tested.value
    if not (
        value_gt is None and value_tested is None
        or (value_gt is not None and value_tested is not None and value_gt == value_tested)
    ):
        raise DifferentPropertyValuesException()

def test_median_finder(algorithm: MedianUpdaterProto,
                       *, n_items: int = 100,
                       upper_bound: float = 1000.0,
                       lower_bound: float = -1000.0,
                       seed: int | None = None) -> bool:
    full_arr: NDArray[float64] = generate_random(
        n_items=n_items, upper_bound=upper_bound, lower_bound=lower_bound, seed=seed
    )

    ground_truth_finder: GroundTruthMedianFinder = GroundTruthMedianFinder()

    # (1) test property getter on an empty array
    try:
        _test_property_access(ground_truth_finder, algorithm)
    except DifferentPropertyValuesException:
        result_msg: str = "FAIL | on property getter | empty array"
        print(result_msg)
        return False

    # (2) iteratively test for non-empty arrays
    for idx, added_value_numpy in enumerate(tqdm(full_arr, desc="Processing items...")): # type: int, float64
        added_value: float = added_value_numpy.item()
        # test update
        median_ground_truth: float = ground_truth_finder.update(added_value)
        median_tested: float = algorithm.update(added_value)
        if median_ground_truth != median_tested:
            result_msg: str = (
                "FAIL | on update"
                f" | item: idx {idx}, value {added_value}"
                f" | median: expected {median_ground_truth}, got {median_tested}"
            )
            print(result_msg)
            return False
        # test property access
        try:
            _test_property_access(ground_truth_finder, algorithm)
        except DifferentPropertyValuesException:
            result_msg: str = (
                "FAIL | on property getter"
                f" | item: idx {idx}, value {added_value}"
            )
            print(result_msg)
            return False

    result_msg: str = "PASS"
    print(result_msg)
    return True