from typing import Iterator, Sequence
import random

def choose_unique_forever[T](items: Sequence[T]) -> Iterator[T]:
    """
    Yield items from `items` in random order, without repetition.
    When the list has been exhausted, sample again and repeat.
    This ensures that, for a list of `items` of length `n`, for `n*k < n < n*(k+1)` calls,
    each item has been yielded at least `k` and at most `k+1` times.
    Useful to ensure that there is as few repetitions of a single item as possible.
    """
    if not items:
        raise ValueError("items must be non-empty")
    while True:
        yield from random.sample(items, k=len(items))
