from typing import List, Self, Any, Iterator, override, cast
import heapq
from abc import ABC, abstractmethod

class BaseHeap[T: int | float](ABC):

    # TODO: implement more generally for comparable types

    def __init__(self, src_list: List[T] | None = None) -> None:
        self._heap: List[T] = src_list if src_list is not None else []

    @staticmethod
    @abstractmethod
    def _transform_item(item: T) -> T:
        pass

    @classmethod
    def _copy_and_transform_list_of_items(cls, items: List[T]) -> List[T]:
        return [cls._transform_item(item) for item in items]

    def __getitem__(self, key: Any) -> T:
        retrieved: T = self._heap[key]
        transformed: T = self._transform_item(retrieved)
        return transformed

    def __iter__(self) -> Iterator[T]:
        for item in self._heap: # type: T
            transformed: T = self._transform_item(item)
            yield transformed

    def __len__(self) -> int:
        return len(self._heap)

    def to_list(self) -> List[T]:
        return list(iter(self))

    @classmethod
    def from_list(cls, src_list: List[T]) -> Self:
        heap: List[T] = cls._copy_and_transform_list_of_items(src_list)
        return cls(heap)

    def clear(self) -> None:
        self._heap.clear()

    def push(self, item: T) -> None:
        transformed: T = self._transform_item(item)
        heapq.heappush(self._heap, transformed)

    def pop(self) -> T:
        popped: T = heapq.heappop(self._heap)
        transformed: T = self._transform_item(popped)
        return transformed

    def pushpop(self, item: T) -> T:
        topush_transformed: T = self._transform_item(item)
        popped: T = heapq.heappushpop(self._heap, topush_transformed)
        popped_transformed: T = self._transform_item(popped)
        return popped_transformed

    def replace(self, item: T) -> T:
        topush_transformed: T = self._transform_item(item)
        popped: T = heapq.heapreplace(self._heap, topush_transformed)
        popped_transformed: T = self._transform_item(popped)
        return popped_transformed

    def peek(self) -> T:
        popped: T = self.pop()
        self.push(popped)
        return popped


class MinHeap[T: int | float](BaseHeap[T]):

    @override
    @staticmethod
    def _transform_item(item: T) -> T:
        return item

class MaxHeap[T: int | float](BaseHeap[T]):

    @override
    @staticmethod
    def _transform_item(item: T) -> T:
        # return cast(T, -item)
        return cast(T, item.__neg__())

def _test_heaps():
    from numpy import float64
    from numpy.typing import NDArray

    from common.utils.numpy_utils import generate_random
    from common.utils.misc_utils import stringify_list_of_floats

    full_arr: NDArray[float64] = generate_random(
        n_items=30, lower_bound=-20.0, upper_bound=20.0, seed=3479
    )
    cur_arr: List[float] = []

    min_heap: MinHeap[float] = MinHeap()
    max_heap: MaxHeap[float] = MaxHeap()

    for idx, value_numpy in enumerate(full_arr): # type: int, float64
        value: float = value_numpy.item()
        cur_arr.append(value)
        cur_arr.sort()
        min_heap.push(value)
        max_heap.push(value)
        print(f"--- {idx} ---")
        print(f"value: {value:.2f}")
        print("sorted array:\n{}".format(stringify_list_of_floats(cur_arr, precision=2)))
        print("min heap:\n{}".format(stringify_list_of_floats(min_heap.to_list(), precision=2)))
        print("max heap:\n{}".format(stringify_list_of_floats(max_heap.to_list(), precision=2)))

if __name__ == "__main__":
    _test_heaps()