from typing import Iterator, Protocol, override

from common.utils.data_structures.heap import BaseHeap, MinHeap, MaxHeap

class MedianUpdaterProto(Protocol):

    """
    A class implementing a dynamic median finder.
    """

    @property
    def value(self) -> float | None:
        """
        Return the current median if defined, or None if not defined.
        """
        ...

    def update(self, value: float) -> float:
        """
        Add value to the sample, recalculate the median and return the new median.
        """
        ...

class DynamicMedianUpdater(MedianUpdaterProto):

    def __init__(self):
        # lowers: max-heap
        # highers: min-heap

        self._lows: MaxHeap[float] = MaxHeap()
        self._highs: MinHeap[float] = MinHeap()

        self._median: float | None = None

    def reset(self):
        self._lows.clear()
        self._highs.clear()
        self._median = None

    def __iter__(self) -> Iterator[float]:
        """
        Iterates over the lows heap, then over the highs heap.
        """
        for heap in (self._lows, self._highs): # type: BaseHeap[float]
            for item in heap: # type: float
                yield item

    @override
    @property
    def value(self) -> float | None:
        return self._median

    @override
    def update(self, value: float) -> float:
        if len(self._lows) == len(self._highs):
            self._lows.push(
                self._highs.pushpop(value)
            )
            self._median = self._lows[0]
        else:
            self._highs.push(
                self._lows.pushpop(value)
            )
            self._median = (self._lows[0] + self._highs[0]) / 2.0
        return self._median